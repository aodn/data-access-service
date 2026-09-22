"""Open zarr stores for the batch job, one at a time, and check each is a
time/lat/lon grid.
"""

from __future__ import annotations

import logging
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
    """Not a lat/lon grid."""


class NoTimeDimensionError(ValueError):
    """No time dimension."""


def _normalise_coords(ds: xr.Dataset, store: str) -> xr.Dataset:
    """Rename coords to time/lat/lon and check the dims."""
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
    """Open ``{store}.zarr`` without dask."""
    key = f"{store}.zarr"
    source = DataQuery.GetAodn().get_dataset(key, chunks=None)
    if not isinstance(source, DataQuery.ZarrDataSource):
        raise TypeError(
            f"Expected ZarrDataSource for {key!r}, got {type(source).__name__}"
        )
    return source


def _open_normalised(store: str) -> ZarrDataSource:
    """Open ``store`` with normalised coords."""
    source = _resolve_zarr_source(store)
    source.zarr_store = _normalise_coords(source.zarr_store, store)
    return source


# The one store this process currently has open, if any.
_open: tuple[str, ZarrDataSource] | None = None


def get_datasource(store: str) -> ZarrDataSource:
    """The store's ``ZarrDataSource``, opened on first use."""
    global _open
    if _open is None or _open[0] != store:
        source = _open_normalised(store)
        logger.info(
            "Store opened: %s (timestamp_count=%d)",
            store,
            source.zarr_store.sizes["time"],
        )
        _open = (store, source)
    return _open[1]


def get_store(store: str) -> xr.Dataset:
    """The store's dataset (time/lat/lon)."""
    return get_datasource(store).zarr_store


def close_store(store: str) -> None:
    """Drop ``store``'s handle."""
    global _open
    if _open is not None and _open[0] == store:
        _open = None


def close_all_stores() -> None:
    """Drop the open handle, whichever store it is (tests)."""
    global _open
    _open = None


def open_store(store: str) -> BaseException | None:
    """Open ``store``. Returns None on success, else the error. Retries
    only errors that might be transient."""
    last_error: BaseException | None = None
    for attempt in range(1, _OPEN_MAX_ATTEMPTS + 1):
        try:
            get_datasource(store)
            return None
        except NotGriddedStoreError as e:
            logger.info(f"Store is not a lat/lon grid, skipping: {store} ({e})")
            return e
        except NoTimeDimensionError as e:
            logger.info(f"Store has no time dimension, skipping: {store} ({e})")
            return e
        except FileNotFoundError as e:
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
