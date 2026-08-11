"""Per-URL registry of long-lived Zarr handles via aodn_cloud_optimised.

Not a cache in the strict sense: handles are not evicted (the URL set is small
and bounded by registered products), and ``ttl`` triggers a background refresh
rather than expiry. Stale entries keep serving until the refresh completes, so
requests never block on freshness — only the very first open per URL blocks.

A per-store ``{local_date: [timestamps]}`` index is built alongside the source
so ``load_slice`` / ``get_available_dates`` can resolve a local date in O(1)
instead of converting every timestamp on the hot path.

Single source of truth is the lib ``ZarrDataSource`` (default chunking, native
coord names for ``get_data``). Callers that need ``time``/``lat``/``lon`` use
``get_store``, which derives a normalised view on demand.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import random
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import anyio
import pandas as pd
import xarray as xr
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import COORD_NAMES
from data_access_service.tiler.utils.dates import DATE_FMT, LOCAL_TZ

if TYPE_CHECKING:
    from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

logger = logging.getLogger(__name__)

_tiler_config = Config.get_config().get_tiler_config()

_STORE_TTL = float(_tiler_config.store_ttl_seconds)

# Capacity gate for concurrent store opens during prewarm. Bounded to the S3
# connection ceiling, not CPU. Runs on the shared anyio pool but a separate
# budget so a many-product startup can't transiently consume tile-handler slots.
_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)

# Bounded: a raw thread per expired store is a stampede at 60 stores.
_REFRESH_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_tiler_config.store_refresh_workers,
    thread_name_prefix="store-refresh",
)

# So stores opened together at startup do not all expire at once.
_REFRESH_JITTER_FRACTION = 0.1

_PREWARM_MAX_ATTEMPTS = 3
_PREWARM_BACKOFF_SECONDS = 1.0


class NotGriddedStoreError(ValueError):
    """The store opened but is not a lat/lon grid. Retrying will not change it."""


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
        raise NotGriddedStoreError(
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
    """Return {local_date: [timestamps]} for the dataset's time coord, or {}.

    Values stay the raw coord elements, since ``_fetch_slice_from_store``
    selects with them.
    """
    if "time" not in ds.dims:
        return {}
    times = ds.coords["time"].values
    if len(times) == 0:
        return {}

    local_dates = (
        pd.DatetimeIndex(times)
        .tz_localize("UTC")
        .tz_convert(LOCAL_TZ)
        .strftime(DATE_FMT)
    )
    index: dict[str, list] = {}
    for local_date, ts in zip(local_dates, times):
        index.setdefault(local_date, []).append(ts)
    return index


class StoreRegistry:
    """See module docstring for the design.

    Concurrent first-time opens of the *same* URL share one open call via a
    per-URL ``concurrent.futures.Future``; opens of *different* URLs run in
    parallel (the original implementation serialised them under a single global
    lock until this pattern was introduced).
    """

    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._sources: dict[str, ZarrDataSource] = {}
        self._opened_at: dict[str, float] = {}
        self._ttl_jitter: dict[str, float] = {}
        self._refreshing: set[str] = set()
        self._in_flight: dict[str, concurrent.futures.Future] = {}
        self._date_index: dict[str, dict[str, list]] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived source for ``store_url``, opening on first request."""
        should_open = False
        with self._lock:
            if store_url in self._sources:
                deadline = self._ttl + self._ttl_jitter.get(store_url, 0.0)
                if time.monotonic() - self._opened_at[store_url] < deadline:
                    return self._sources[store_url]
                # Serve stale, refresh once in the background.
                if store_url not in self._refreshing:
                    self._refreshing.add(store_url)
                    logger.info(
                        f"Store TTL expired, refreshing in background: {store_url}"
                    )
                    _REFRESH_EXECUTOR.submit(self._refresh_background, store_url)
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

    def cached(self, store_url: str) -> xr.Dataset | None:
        """Already-open normalised dataset, or None. Never opens, never refreshes."""
        with self._lock:
            source = self._sources.get(store_url)
        if source is None:
            return None
        return _normalise_coords(source.zarr_store, store_url)

    def date_index(self, store_url: str) -> dict[str, list]:
        """Return the {local_date: [timestamps]} map for ``store_url`` (or empty dict)."""
        with self._lock:
            return self._date_index.get(store_url, {})

    async def _prewarm_one(self, store_url: str) -> BaseException | None:
        """Open one URL. None on success, else the exception.

        Not-a-grid and not-there are confirmed and not retried; anything else
        gets bounded retries with backoff.
        """
        last_error: BaseException | None = None
        for attempt in range(1, _PREWARM_MAX_ATTEMPTS + 1):
            try:
                await anyio.to_thread.run_sync(
                    self._ensure_open, store_url, limiter=_STORE_PREWARM_LIMITER
                )
                return None
            except NotGriddedStoreError as e:
                logger.info(f"Store is not a lat/lon grid, skipping: {store_url} ({e})")
                return e
            except FileNotFoundError as e:
                # Usually an upstream rename the catalogue hasn't caught up with.
                logger.warning(f"Store does not exist: {store_url} ({e})")
                return e
            except Exception as e:
                last_error = e
                if attempt < _PREWARM_MAX_ATTEMPTS:
                    delay = _PREWARM_BACKOFF_SECONDS * 2 ** (attempt - 1)
                    logger.warning(
                        f"Store open failed (attempt {attempt}/{_PREWARM_MAX_ATTEMPTS}), "
                        f"retrying in {delay:.1f}s: {store_url} ({e!r})"
                    )
                    await anyio.sleep(delay)
                else:
                    logger.error(
                        f"Store open failed after {_PREWARM_MAX_ATTEMPTS} attempts: "
                        f"{store_url}",
                        exc_info=e,
                    )
        return last_error

    async def prewarm(self, store_urls: list[str]) -> dict[str, BaseException | None]:
        """Open every URL in parallel and report the per-URL outcome.

        Moves the one-time S3 metadata cost from the first user request to server
        startup, and lets get_products_availability respond fast on first call.

        Returns ``{url: None on success, else the exception}``.
        """
        outcomes: dict[str, BaseException | None] = {}
        logger.debug("Prewarming %d stores: %s", len(store_urls), store_urls)

        async def _one(url: str) -> None:
            outcomes[url] = await self._prewarm_one(url)

        await asyncio.gather(*(_one(url) for url in store_urls))

        not_gridded = sum(
            1 for e in outcomes.values() if isinstance(e, NotGriddedStoreError)
        )
        absent = sum(1 for e in outcomes.values() if isinstance(e, FileNotFoundError))
        unresolved = sum(
            1
            for e in outcomes.values()
            if e is not None
            and not isinstance(e, (NotGriddedStoreError, FileNotFoundError))
        )
        logger.info(
            "Store prewarm complete: %d opened, %d not gridded, %d absent, "
            "%d unresolved (of %d)",
            len(outcomes) - not_gridded - absent - unresolved,
            not_gridded,
            absent,
            unresolved,
            len(outcomes),
        )
        return outcomes

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._sources.clear()
            self._opened_at.clear()
            self._ttl_jitter.clear()
            self._refreshing.clear()
            self._in_flight.clear()
            self._date_index.clear()

    def _publish(
        self,
        store_url: str,
        source: ZarrDataSource,
        index: dict[str, list],
    ) -> None:
        """Atomically replace source, opened-at timestamp, and date index for a URL."""
        with self._lock:
            self._sources[store_url] = source
            self._opened_at[store_url] = time.monotonic()
            self._ttl_jitter[store_url] = random.uniform(
                0.0, self._ttl * _REFRESH_JITTER_FRACTION
            )
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


def cached_store(store_url: str) -> xr.Dataset | None:
    return store_registry.cached(store_url)


def get_available_dates(store_url: str) -> list[str]:
    get_store(store_url)  # ensures the date index for this URL is populated
    index = store_registry.date_index(store_url)
    return sorted(index) if index else []


async def prewarm_stores(store_urls: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every URL and return the per-URL outcome map."""
    return await store_registry.prewarm(store_urls)
