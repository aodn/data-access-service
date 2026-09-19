"""Load a (store, timestamp, variables) slice from parquet as a dense
(lat, lon) dataset. Concurrent identical loads share one read, and results
are cached in L1 (``slice_cache``)."""

import threading

import pandas as pd
import xarray as xr

from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.caching.slice_cache import slice_memo
from data_access_service.tiler.services.rendering.masks import apply_ocean_mask
from data_access_service.tiler.services.store.registry import (
    get_store_metadata,
    resolve_timestamp,
    unavailable_date_message,
)
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
    _get_client,
)

_slice_dedup = Deduper()

_COLD_READ_LIMIT = threading.BoundedSemaphore(4)


def _warm_coord_indexes(ds: xr.Dataset) -> xr.Dataset:
    """Build the lat/lon indexes now, not on first use."""
    for dim in ("lon", "lat"):
        if dim in ds.indexes:
            ds.indexes[dim].is_unique
    return ds


def _compute_slice_from_store(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """Read the slice, applying the ocean mask if ``ocean_masked``."""
    result = _fetch_slice_from_store(store, ts, variables)
    if ocean_masked:
        result = apply_ocean_mask(result, variables)
    return result


def _fetch_slice_from_store(
    store: str, ts: pd.Timestamp, variables: list[str]
) -> xr.Dataset:
    meta = get_store_metadata(store)

    missing = [v for v in variables if v not in meta.variables]
    if missing:
        raise FileNotFoundError(
            f"Variable(s) {missing} not found in store {store!r} "
            f"(available: {sorted(meta.variables)})"
        )

    raw_ts = resolve_timestamp(store, ts)
    if raw_ts is None:
        raise FileNotFoundError(unavailable_date_message(store, ts))

    with _COLD_READ_LIMIT:
        repo = TilerParquetRepository(_get_client())
        data_vars = {}
        for v in variables:
            var_meta = meta.variables[v]
            arr = repo.fetch_variable_slice(
                store, v, raw_ts, meta.n_i, meta.n_j, var_meta.dtype
            )
            data_vars[v] = xr.DataArray(
                arr, dims=("lat", "lon"), attrs=dict(var_meta.attrs)
            )

    return xr.Dataset(data_vars, coords={"lat": meta.lat, "lon": meta.lon})


def load_slice(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """The slice at exact instant ``ts``, cached in L1. ``ocean_masked`` is
    fixed per product, so it isn't part of the cache key."""
    cache_key = (store, ts, tuple(sorted(variables)))

    def compute() -> xr.Dataset:
        result = slice_memo.get_or_compute(
            cache_key,
            lambda: _compute_slice_from_store(store, ts, variables, ocean_masked),
        )

        return _warm_coord_indexes(result)

    return _slice_dedup.dedupe(cache_key, compute)


def load_slice_uncached(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """The slice without L1, for animations, so they don't evict hot slices."""
    return _compute_slice_from_store(store, ts, variables, ocean_masked)
