"""Per-store registry of long-lived Zarr handles via aodn_cloud_optimised.

Batch-only: the one place that still opens real zarr stores. The live tiler
API never touches zarr — it reads what this module's caller
(``parquet_generator``/``generator``) publishes instead (``root_metadata.json``
+ each store's ``metadata.json`` sidecar).

Also validates: ``open_store`` checks a store is a lat/lon grid with a time
dimension and reports the outcome, so ``generator`` can skip broken stores.
Stores are opened one at a time, each in its own forked worker, so only one
zarr is ever held in memory.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import xarray as xr
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.config.tiler.constants import COORD_NAMES

if TYPE_CHECKING:
    from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

logger = logging.getLogger(__name__)

_OPEN_MAX_ATTEMPTS = 3
_OPEN_BACKOFF_SECONDS = 1.0


class NotGriddedStoreError(ValueError):
    """The store opened but is not a lat/lon grid. Retrying will not change it."""


class NoTimeDimensionError(ValueError):
    """The store opened but has no time dimension; every date request would 404."""


def _normalise_coords(ds: xr.Dataset, store: str) -> xr.Dataset:
    """Rename TIME/LATITUDE/LONGITUDE → time/lat/lon and validate dims."""
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise NotGriddedStoreError(
            f"Store {store!r} missing lat/lon dims after rename (found: {list(ds.dims)})"
        )
    if "time" not in ds.dims:
        raise NoTimeDimensionError(
            f"Store {store!r} has no time dimension; every date request would 404"
        )
    return ds.sortby("time")


def _resolve_zarr_source(store: str) -> ZarrDataSource:
    """Open a ZarrDataSource via aodn_cloud_optimised, dask disabled.

    ``chunks=None``: reads are single-slice + eager ``.compute()``, so a dask
    graph just costs open-time memory for nothing.
    """
    key = f"{store}.zarr"
    source = DataQuery.GetAodn().get_dataset(key, chunks=None)
    if not isinstance(source, DataQuery.ZarrDataSource):
        raise TypeError(
            f"Expected ZarrDataSource for {key!r}, got {type(source).__name__}"
        )
    return source


def _open_store(store: str) -> ZarrDataSource:
    """Resolve via lib and normalise its dataset in place to time/lat/lon."""
    source = _resolve_zarr_source(store)
    # Overwrite in place so every later reader sees the normalised view.
    source.zarr_store = _normalise_coords(source.zarr_store, store)
    return source


class StoreRegistry:
    """See module docstring for the design. No same-store dedup — the batch
    job never requests one store twice at once.
    """

    def __init__(self) -> None:
        self._stores: dict[str, ZarrDataSource] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store: str) -> ZarrDataSource:
        """Return the long-lived source for ``store``, opening on first request."""
        with self._lock:
            source = self._stores.get(store)
        if source is not None:
            return source

        source = _open_store(store)
        logger.info(
            "Store opened: %s (timestamp_count=%d)",
            store,
            source.zarr_store.sizes["time"],
        )
        self._publish(store, source)
        return source

    def get(self, store: str) -> xr.Dataset:
        """Return a normalised (time/lat/lon) view, opening the source if needed."""
        return self._ensure_open(store).zarr_store

    def get_datasource(self, store: str) -> ZarrDataSource:
        """Return the long-lived ``ZarrDataSource`` for ``store`` (opens if needed)."""
        return self._ensure_open(store)

    def close(self, store: str) -> None:
        """Drop ``store``'s handle, so an in-process run holds one at a time."""
        with self._lock:
            self._stores.pop(store, None)

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._stores.clear()

    def _publish(self, store: str, source: ZarrDataSource) -> None:
        """Publish the opened source for a store."""
        with self._lock:
            self._stores[store] = source


store_registry = StoreRegistry()


def get_store(store: str) -> xr.Dataset:
    return store_registry.get(store)


def get_datasource(store: str) -> ZarrDataSource:
    return store_registry.get_datasource(store)


def close_store(store: str) -> None:
    store_registry.close(store)


def open_store(store: str) -> BaseException | None:
    """Open ``store`` and confirm it can be converted. None on success, else
    the exception.

    Not-a-grid, not-there, and no-time-dimension are confirmed and not
    retried; anything else gets bounded retries with backoff.
    """
    last_error: BaseException | None = None
    for attempt in range(1, _OPEN_MAX_ATTEMPTS + 1):
        try:
            store_registry.get(store)
            return None
        except NotGriddedStoreError as e:
            logger.info(f"Store is not a lat/lon grid, skipping: {store} ({e})")
            return e
        except NoTimeDimensionError as e:
            logger.info(f"Store has no time dimension, skipping: {store} ({e})")
            return e
        except FileNotFoundError as e:
            # Usually an upstream rename the catalogue hasn't caught up with.
            logger.warning(f"Store does not exist: {store} ({e})")
            return e
        except Exception as e:
            last_error = e
            if attempt < _OPEN_MAX_ATTEMPTS:
                delay = _OPEN_BACKOFF_SECONDS * 2 ** (attempt - 1)
                logger.warning(
                    f"Store open failed (attempt {attempt}/{_OPEN_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s: {store} ({e!r})"
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Store open failed after {_OPEN_MAX_ATTEMPTS} attempts: {store}",
                    exc_info=e,
                )
    return last_error
