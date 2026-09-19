"""Slice loading.

``load_slice`` returns a fully-computed 2-D slice for a (store, timestamp,
variables) tuple. Concurrent identical requests always share one compute
in-process via ``_slice_dedup`` (independent of ``CACHE_BACKEND``); when
``CACHE_BACKEND=redis``, ``slice_memo`` additionally coalesces across
instances and caches the result (see ``services.caching.slice_cache``).

Callers pass an already-parsed ``pd.Timestamp`` (from
``core.tiler_routes.shared.parse_date_or_422``), not a raw date string —
every route handler parses/validates its ``date`` query param exactly once,
so this module never re-parses a string it was already handed as a
``pd.Timestamp``.

No zarr here: a slice is read straight from the batch-generated parquet
files via duckdb, reconstructing the dense ``(lat, lon)`` array from the
sparse ``(timestamp, i, j, value)`` rows using the sidecar's shape/coords
(``store.registry.get_store_metadata``). Store handles/metadata live in their
own module ([[store.registry]]); the actual read SQL — and the shared
``TilerDuckDBClient`` — live in ``TilerParquetRepository`` ([[tiler_repository]]).
"""

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

# Always in-process, independent of CACHE_BACKEND — see Deduper's docstring
# for why this matters even (especially) under CACHE_BACKEND=none.
_slice_dedup = Deduper()


def _warm_coord_indexes(ds: xr.Dataset) -> xr.Dataset:
    """Force-build the lazy pandas index engine for lat/lon, once, here."""
    for dim in ("lon", "lat"):
        if dim in ds.indexes:
            ds.indexes[dim].is_unique
    return ds


def _compute_slice_from_store(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> xr.Dataset:
    """Fetch a 2-D slice from the store's parquet files. Both `load_slice` and
    `load_slice_uncached` delegate here; they differ only in whether the
    result lands in L1.

    When ``ocean_masked`` is set, anomalous values outside the model's valid ocean
    domain are nulled here (masks.apply_ocean_mask) so every downstream consumer
    inherits the cut.
    """
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
    """
    Return a fully-computed 2D (lat × lon) slice for the given store, timestamp,
    and variables. ``ts`` must name an exact instant in the store's time index —
    no nearest-match fallback. Coordinate names are normalised to
    ``time``/``lat``/``lon`` before return.

    ``ocean_masked`` (from ``Product.ocean_masked``) nulls anomalous values outside
    the valid model domain. It's a deterministic function of the cache key (a store
    + variable set maps to one product), so it stays out of the key; the masked
    slice is what L1 caches.
    """
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
    """Return a 2-D slice without touching L1.

    Used by the animation endpoint so a rare multi-date request doesn't evict
    another product's hot slices from the shared L1 cache (CACHE_BACKEND=redis).
    """
    return _compute_slice_from_store(store, ts, variables, ocean_masked)
