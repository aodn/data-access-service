"""Slice loading.

``load_slice`` returns a fully-computed 2-D slice for a (store, date,
variables) tuple. Concurrent identical requests always share one compute
in-process via ``_slice_dedup`` (independent of ``CACHE_BACKEND``); when
``CACHE_BACKEND=redis``, ``slice_memo`` additionally coalesces across
instances and caches the result (see ``services.caching.slice_cache``).

Long-lived store handles live in their own module ([[store.registry]]).
Time selection goes through ``aodn_cloud_optimised`` ``ZarrDataSource.get_data``.
"""

import pandas as pd
import xarray as xr

from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.caching.slice_cache import slice_memo
from data_access_service.tiler.services.rendering.masks import apply_ocean_mask
from data_access_service.tiler.services.store.registry import (
    get_datasource,
    get_store,
    resolve_timestamp,
    store_registry,
)

# Always in-process, independent of CACHE_BACKEND — see Deduper's docstring
# for why this matters even (especially) under CACHE_BACKEND=none.
_slice_dedup = Deduper()


def _ts_for_get_data(ts) -> str:
    """Format a timestamp for ``ZarrDataSource.get_data`` date bounds."""
    return pd.Timestamp(ts).isoformat()


def _compute_slice_from_store(
    store_url: str, date: str, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """Fetch a 2-D slice from the Zarr store. Both `load_slice` and
    `load_slice_uncached` delegate here; they differ only in whether the
    result lands in L1.

    When ``ocean_masked`` is set, anomalous values outside the model's valid ocean
    domain are nulled here (masks.apply_ocean_mask) so every downstream consumer
    inherits the cut.
    """
    result = _fetch_slice_from_store(store_url, date, variables)
    if ocean_masked:
        result = apply_ocean_mask(result, variables)
    return result


def _fetch_slice_from_store(
    store_url: str, date: str, variables: list[str]
) -> xr.Dataset:
    # Ensure store is open (time index + variable catalogue on normalised view).
    store = get_store(store_url)

    missing = [v for v in variables if v not in store.data_vars]
    if missing:
        raise FileNotFoundError(
            f"Variable(s) {missing} not found in store {store_url!r} "
            f"(available: {sorted(store.data_vars)})"
        )

    t0 = resolve_timestamp(store_url, date)
    if t0 is None:
        index = store_registry.time_index(store_url)
        latest = index[max(index)][1] if index else None
        hint = (
            f" Latest available date is {latest!r}."
            if latest
            else " No dates are available."
        )
        raise FileNotFoundError(f"No data for date {date!r}.{hint}")

    try:
        ds = get_datasource(store_url).get_data(
            date_start=_ts_for_get_data(t0),
            date_end=_ts_for_get_data(t0),
        )
        ds = ds[variables]
        # get_data returns a time range (often length 1). Match previous
        # .sel(time=scalar) behaviour: one frame, time dim dropped.
        if "time" in ds.dims:
            if ds.sizes["time"] == 0:
                raise KeyError(date)
            ds = ds.isel(time=0)
        return ds.compute() if hasattr(ds, "compute") else ds
    except KeyError as e:
        raise FileNotFoundError(f"No data found for date {date}") from e


def load_slice(
    store_url: str, date: str, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """
    Return a fully-computed 2D (lat × lon) slice for the given store, date, and variables.
    Uses nearest-match on time so callers don't need to ask exact timestamps.
    Coordinate names are normalised to ``time``/``lat``/``lon`` before return.

    ``ocean_masked`` (from ``Product.ocean_masked``) nulls anomalous values outside
    the valid model domain. It's a deterministic function of the cache key (a store
    + variable set maps to one product), so it stays out of the key; the masked
    slice is what L1 caches.
    """
    cache_key = (store_url, date, tuple(sorted(variables)))

    def compute() -> xr.Dataset:
        return slice_memo.get_or_compute(
            cache_key,
            lambda: _compute_slice_from_store(store_url, date, variables, ocean_masked),
        )

    return _slice_dedup.dedupe(cache_key, compute)


def load_slice_uncached(
    store_url: str, date: str, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """Return a 2-D slice without touching L1.

    Pulls via lib ``get_data``. Used by the animation endpoint so a
    rare multi-date request doesn't evict another product's hot slices from
    the shared L1 cache (CACHE_BACKEND=redis).
    """
    return _compute_slice_from_store(store_url, date, variables, ocean_masked)
