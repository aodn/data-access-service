"""Per-URL registry of long-lived Zarr handles via aodn_cloud_optimised.

Not a cache in the strict sense: handles are not evicted (the URL set is small
and bounded by registered products), and ``ttl`` triggers a background refresh
rather than expiry. Stale entries keep serving until the refresh completes, so
requests never block on freshness — only the very first open per URL blocks.

A per-store ``{local_date: [timestamps]}`` index is built on open so
``load_slice`` / ``get_available_dates`` can resolve a local date in O(1)
instead of converting every timestamp on the hot path.

Single source of truth is the lib ``ZarrDataSource`` (default chunking,
native coord names for ``get_data``). Callers that need ``time``/``lat``/``lon``
use ``get_store``, which derives a normalised view on demand.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import anyio
import xarray as xr
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import COORD_NAMES
from data_access_service.tiler.utils.dates import ts_to_local_date

if TYPE_CHECKING:
    from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

logger = logging.getLogger(__name__)

_tiler_config = Config.get_config().get_tiler_config()

_STORE_TTL = float(_tiler_config.store_ttl_seconds)

# Capacity gate for concurrent store opens during prewarm. Bounded to the S3
# connection ceiling, not CPU. Runs on the shared anyio pool but a separate
# budget so a many-product startup can't transiently consume tile-handler slots.
_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)


def _dataset_key_from_url(store_url: str) -> str:
    """Map a product ``source_path`` to the lib dataset key (``name.zarr``).

    Examples:
      ``s3://aodn-cloud-optimised/foo.zarr/`` → ``foo.zarr``
      ``s3://bucket/prefix/foo.zarr`` → ``foo.zarr``
    """
    path = urlparse(store_url).path if "://" in store_url else store_url
    key = path.rstrip("/").rsplit("/", 1)[-1]
    if not key.endswith(".zarr"):
        raise ValueError(
            f"Cannot derive dataset key from store URL {store_url!r} "
            f"(expected a path ending in '.zarr')"
        )
    return key


def _normalise_coords(ds: xr.Dataset, store_url: str) -> xr.Dataset:
    """Rename TIME/LATITUDE/LONGITUDE → time/lat/lon and validate spatial dims."""
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise ValueError(
            f"Store {store_url!r} missing lat/lon dims after rename (found: {list(ds.dims)})"
        )
    if "time" in ds.dims:
        ds = ds.sortby("time")
    return ds


def _resolve_zarr_source(store_url: str) -> ZarrDataSource:
    """Open a ZarrDataSource via aodn_cloud_optimised (lib default chunking)."""
    key = _dataset_key_from_url(store_url)
    source = DataQuery.GetAodn().get_dataset(key)
    if not isinstance(source, DataQuery.ZarrDataSource):
        raise TypeError(
            f"Expected ZarrDataSource for {key!r}, got {type(source).__name__}"
        )
    return source


def _open_source(store_url: str) -> ZarrDataSource:
    """Resolve via lib and validate that coords normalise to lat/lon."""
    source = _resolve_zarr_source(store_url)
    _normalise_coords(source.zarr_store, store_url)  # raises if spatial dims missing
    return source


def _build_date_index(ds: xr.Dataset) -> dict[str, list]:
    """Return {local_date: [timestamps]} for the dataset's time coord, or {} if missing."""
    if "time" not in ds.dims:
        return {}
    index: dict[str, list] = {}
    for ts in ds.coords["time"].values:
        index.setdefault(ts_to_local_date(ts), []).append(ts)
    return index


class StoreRegistry:
    """See module docstring for the design.

    Concurrent first-time opens of the *same* URL share one open call via a
    per-URL ``concurrent.futures.Future``; opens of *different* URLs run in
    parallel.
    """

    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._sources: dict[str, ZarrDataSource] = {}
        self._opened_at: dict[str, float] = {}
        self._refreshing: set[str] = set()
        self._in_flight: dict[str, concurrent.futures.Future] = {}
        self._date_index: dict[str, dict[str, list]] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived source for ``store_url``, opening on first request."""
        should_open = False
        with self._lock:
            if store_url in self._sources:
                if time.monotonic() - self._opened_at[store_url] < self._ttl:
                    return self._sources[store_url]
                # TTL expired — return stale source and trigger a background refresh.
                if store_url not in self._refreshing:
                    self._refreshing.add(store_url)
                    logger.info(
                        f"Store TTL expired, refreshing in background: {store_url}"
                    )
                    threading.Thread(
                        target=self._refresh_background, args=(store_url,), daemon=True
                    ).start()
                return self._sources[store_url]
            if store_url in self._in_flight:
                future = self._in_flight[store_url]
            else:
                future = concurrent.futures.Future()
                self._in_flight[store_url] = future
                should_open = True

        if not should_open:
            return future.result()

        try:
            source = _open_source(store_url)
            index = _build_date_index(_normalise_coords(source.zarr_store, store_url))
            self._publish(store_url, source, index)
            logger.info(f"Store opened: {store_url} (date_count={len(index)})")
            future.set_result(source)
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            with self._lock:
                self._in_flight.pop(store_url, None)
        return source

    def get(self, store_url: str) -> xr.Dataset:
        """Return a normalised (time/lat/lon) view, opening the source if needed."""
        source = self._ensure_open(store_url)
        return _normalise_coords(source.zarr_store, store_url)

    def get_datasource(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived ``ZarrDataSource`` for ``store_url`` (opens if needed)."""
        return self._ensure_open(store_url)

    def date_index(self, store_url: str) -> dict[str, list]:
        """Return the {local_date: [timestamps]} map for ``store_url`` (or empty dict)."""
        with self._lock:
            return self._date_index.get(store_url, {})

    async def prewarm(self, store_urls: list[str]) -> None:
        """Open every URL in parallel via the anyio thread pool.

        Moves the one-time S3 metadata cost from the first user request to server
        startup, and lets get_products_availability respond fast on first call.
        Per-URL failures are logged and swallowed so a single bad URL doesn't
        block the others.
        """

        async def _one(url: str) -> None:
            try:
                await anyio.to_thread.run_sync(
                    self._ensure_open, url, limiter=_STORE_PREWARM_LIMITER
                )
            except Exception:
                logger.exception(f"Store prewarm failed: {url}")

        await asyncio.gather(*(_one(url) for url in store_urls))

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._sources.clear()
            self._opened_at.clear()
            self._refreshing.clear()
            self._in_flight.clear()
            self._date_index.clear()

    def _publish(
        self,
        store_url: str,
        source: ZarrDataSource,
        index: dict[str, list],
    ) -> None:
        """Atomically replace source, opened-at, and date index for a URL."""
        with self._lock:
            self._sources[store_url] = source
            self._opened_at[store_url] = time.monotonic()
            self._date_index[store_url] = index

    def _refresh_background(self, store_url: str) -> None:
        try:
            source = _open_source(store_url)
            index = _build_date_index(_normalise_coords(source.zarr_store, store_url))
            self._publish(store_url, source, index)
            logger.info(f"Store refreshed: {store_url}")
        except Exception:
            logger.exception(f"Background refresh failed: {store_url}")
        finally:
            with self._lock:
                self._refreshing.discard(store_url)


store_registry = StoreRegistry(_STORE_TTL)


def get_store(store_url: str) -> xr.Dataset:
    return store_registry.get(store_url)


def get_datasource(store_url: str) -> ZarrDataSource:
    return store_registry.get_datasource(store_url)


def get_available_dates(store_url: str) -> list[str]:
    get_store(store_url)  # ensures the date index for this URL is populated
    index = store_registry.date_index(store_url)
    return sorted(index) if index else []


async def prewarm_stores(store_urls: list[str]) -> None:
    try:
        await store_registry.prewarm(store_urls)
    except Exception:
        logger.exception("Store prewarm task exited with error")
