"""Per-URL registry of long-lived Zarr handles via aodn_cloud_optimised.

Batch-only: the one place that still opens real zarr stores. The live tiler
API never touches zarr — it reads what this module's caller
(``parquet_generator``/``generator``) publishes instead (``root_metadata.json``
+ each store's ``metadata.json`` sidecar).

Also validates: ``prewarm`` checks each store is a lat/lon grid with a time
dimension and reports the outcome, so ``generator`` can skip broken stores.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import anyio
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
    """Rename TIME/LATITUDE/LONGITUDE → time/lat/lon and validate dims."""
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
    """Open a ZarrDataSource via aodn_cloud_optimised, dask disabled.

    ``chunks=None``: reads are single-slice + eager ``.compute()``, so a dask
    graph just costs open-time memory for nothing.
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
    # Overwrite in place so every later reader sees the normalised view.
    source.zarr_store = _normalise_coords(source.zarr_store, store_url)
    return source


class StoreRegistry:
    """See module docstring for the design.

    Different URLs open in parallel (bounded by ``_STORE_PREWARM_LIMITER``).
    No same-URL dedup — the batch job never requests one URL twice at once.
    """

    def __init__(self) -> None:
        self._stores: dict[str, ZarrDataSource] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived source for ``store_url``, opening on first request."""
        with self._lock:
            source = self._stores.get(store_url)
        if source is not None:
            return source

        source = _open_store(store_url)
        logger.info(
            "Store opened: %s (timestamp_count=%d)",
            store_url,
            source.zarr_store.sizes["time"],
        )
        self._publish(store_url, source)
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
