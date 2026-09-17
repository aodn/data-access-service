"""Per-URL registry of long-lived Zarr handles via aodn_cloud_optimised.

Batch-only: this is the one place in the codebase that still opens real zarr
stores. The live tiler API never touches zarr - it reads what this module's
caller (``batch.tiler.parquet_generator``/``generator``) publishes instead
(``root_metadata.json`` + each store's ``metadata.json`` sidecar), via its own
``tiler.services.store.registry``.

Not a cache in the strict sense: handles are not evicted (the URL set is
bounded by the discovered product catalogue) and are never refreshed from a
request path - each batch run opens what it needs once. Timestamp resolution
(exact UTC instant, not calendar day — see ``tiler/technical.md`` §9) is a
live-API concern (``tiler.services.store.registry``, reading ``metadata.json``
sidecars this job writes); the batch job itself just reads a store's full
``time`` coordinate directly (see ``parquet_generator.generate_parquet``), so
this module builds no timestamp index of its own.

Single source of truth is the lib ``ZarrDataSource`` (opened with
``chunks=None`` so dask is not built at open time; native coord names for
``get_data``). Callers that need ``time``/``lat``/``lon`` use ``get_store``,
which derives a normalised view on demand.

``prewarm`` also decides whether a store is fit to serve tiler requests at
all (grid shape, time dimension) and reports the per-URL outcome directly to
its caller (``generator.generate_tiler_parquet_for_all_products``), which
uses that return value to skip stores that failed — there is no separate
"is this store healthy" query elsewhere, since a batch run only ever prewarms
once per store per run.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import anyio
import pandas as pd
import xarray as xr
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import COORD_NAMES

if TYPE_CHECKING:
    from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

logger = logging.getLogger(__name__)

_tiler_config = Config.get_config().get_tiler_config()

_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)
_PREWARM_MAX_ATTEMPTS = 3
_PREWARM_BACKOFF_SECONDS = 1.0


class NotGriddedStoreError(ValueError):
    """The store opened but is not a lat/lon grid. Retrying will not change it."""


class NoTimeDimensionError(ValueError):
    """The store opened but has no time dimension; every date request would 404."""


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
    """Rename TIME/LATITUDE/LONGITUDE → time/lat/lon and validate dims.

    Every caller of ``_ensure_open`` — prewarm and lazy opens alike — needs a
    grid with a time axis, so both checks live here rather than being
    re-derived per caller.
    """
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise NotGriddedStoreError(
            f"Store {store_url!r} missing lat/lon dims after rename (found: {list(ds.dims)})"
        )
    if "time" not in ds.dims:
        raise NoTimeDimensionError(
            f"Store {store_url!r} has no time dimension; every date request would 404"
        )
    return ds.sortby("time")


def _resolve_zarr_source(store_url: str) -> ZarrDataSource:
    """Open a ZarrDataSource via aodn_cloud_optimised with dask disabled.

    ``chunks=None`` is required: every tiler read is a single-slice
    ``get_data`` + eager ``.compute()``, so dask's task graph buys nothing
    while costing open-time memory on finely-chunked production stores
    (10M+ graph tasks / tens of GB just to describe layout). xarray still
    indexes Zarr lazily and only fetches native chunks on ``.compute()``.
    """
    key = _dataset_key_from_url(store_url)
    source = DataQuery.GetAodn().get_dataset(key, chunks=None)
    if not isinstance(source, DataQuery.ZarrDataSource):
        raise TypeError(
            f"Expected ZarrDataSource for {key!r}, got {type(source).__name__}"
        )
    return source


def _open_store(store_url: str) -> ZarrDataSource:
    """Resolve via lib and normalise its dataset in place to time/lat/lon."""
    source = _resolve_zarr_source(store_url)
    # `source.zarr_store` already holds the full dataset (native TIME/LATITUDE/
    # LONGITUDE names) the moment the lib opens it; this just overwrites that
    # same attribute with the renamed/sorted view, once, so every later reader
    # (ours and the lib's own get_data) sees time/lat/lon without recomputing it.
    source.zarr_store = _normalise_coords(source.zarr_store, store_url)
    return source


class StoreRegistry:
    """See module docstring for the design.

    Concurrent first-time opens of the *same* URL share one open call via a
    per-URL ``concurrent.futures.Future``; opens of *different* URLs run in
    parallel (the original implementation serialised them under a single global
    lock until this pattern was introduced).
    """

    def __init__(self) -> None:
        self._stores: dict[str, ZarrDataSource] = {}
        self._in_flight: dict[str, concurrent.futures.Future] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived source for ``store_url``, opening on first request."""
        should_open = False
        with self._lock:
            if store_url in self._stores:
                return self._stores[store_url]
            if store_url in self._in_flight:
                future = self._in_flight[store_url]
            else:
                future = concurrent.futures.Future()
                self._in_flight[store_url] = future
                should_open = True

        if not should_open:
            return future.result()

        try:
            source = _open_store(store_url)
            self._publish(store_url, source)
            logger.info(
                "Store opened: %s (timestamp_count=%d)",
                store_url,
                source.zarr_store.sizes["time"],
            )
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
        return self._ensure_open(store_url).zarr_store

    def get_datasource(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived ``ZarrDataSource`` for ``store_url`` (opens if needed)."""
        return self._ensure_open(store_url)

    async def _prewarm_one(self, store_url: str) -> BaseException | None:
        """Open one URL and confirm it can serve tiler requests. None on
        success, else the exception.

        Not-a-grid, not-there, and no-time-dimension are confirmed and not
        retried; anything else gets bounded retries with backoff.
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
            except NoTimeDimensionError as e:
                logger.info(f"Store has no time dimension, skipping: {store_url} ({e})")
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
        no_time = sum(
            1 for e in outcomes.values() if isinstance(e, NoTimeDimensionError)
        )
        unresolved = sum(
            1
            for e in outcomes.values()
            if e is not None
            and not isinstance(
                e, (NotGriddedStoreError, FileNotFoundError, NoTimeDimensionError)
            )
        )
        logger.info(
            "Store prewarm complete: %d opened, %d not gridded, %d absent, "
            "%d no time dimension, %d unresolved (of %d)",
            len(outcomes) - not_gridded - absent - no_time - unresolved,
            not_gridded,
            absent,
            no_time,
            unresolved,
            len(outcomes),
        )
        return outcomes

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._stores.clear()
            self._in_flight.clear()

    def _publish(self, store_url: str, source: ZarrDataSource) -> None:
        """Publish the opened source for a URL."""
        with self._lock:
            self._stores[store_url] = source


store_registry = StoreRegistry()


def get_store(store_url: str) -> xr.Dataset:
    return store_registry.get(store_url)


def get_datasource(store_url: str) -> ZarrDataSource:
    return store_registry.get_datasource(store_url)


async def prewarm_stores(store_urls: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every URL and return the per-URL outcome map."""
    return await store_registry.prewarm(store_urls)
