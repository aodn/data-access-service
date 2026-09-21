"""Load a (store, timestamp, variables) slice from parquet as CSR
(``SparseSlice``), cached in L1 (``slice_cache``). Renderers build only the
part they need. Concurrent identical loads share one read."""

import threading

import numpy as np
import pandas as pd

from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.caching.slice_cache import slice_memo
from data_access_service.tiler.services.rendering.masks import ocean_valid_for_coords
from data_access_service.tiler.services.store.registry import (
    get_store_metadata,
    resolve_timestamp,
    unavailable_date_message,
)
from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    SparseSlice,
)
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
    _get_client,
)

_slice_dedup = Deduper()

# Cold reads at once. Each one holds a whole slice while it builds, so this
# caps what a burst of misses can allocate.
COLD_READ_CONCURRENCY = 2
_COLD_READ_LIMIT = threading.BoundedSemaphore(COLD_READ_CONCURRENCY)


def _compute_slice_from_store(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> dict[str, SparseGrid]:
    """Read the slice, applying the ocean mask if ``ocean_masked``."""
    grids = _fetch_slice_from_store(store, ts, variables)
    if ocean_masked:
        meta = get_store_metadata(store)
        valid = ocean_valid_for_coords(meta.lon, meta.lat)
        grids = {v: grid.keep(valid) for v, grid in grids.items()}
    return grids


def _fetch_slice_from_store(
    store: str, ts: pd.Timestamp, variables: list[str]
) -> dict[str, SparseGrid]:
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
        return {
            v: repo.fetch_variable_slice(
                store, v, raw_ts, meta.n_i, meta.n_j, meta.variables[v].dtype
            )
            for v in variables
        }


def _to_sparse_slice(store: str, grids: dict[str, SparseGrid]) -> SparseSlice:
    meta = get_store_metadata(store)
    return SparseSlice(
        lat=np.asarray(meta.lat),
        lon=np.asarray(meta.lon),
        grids=grids,
        attrs={v: dict(meta.variables[v].attrs) for v in grids},
    )


def load_slice(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> SparseSlice:
    """The slice at exact instant ``ts``, cached in L1. ``ocean_masked`` is
    fixed per product, so it isn't part of the cache key."""
    cache_key = (store, ts, tuple(sorted(variables)))

    def compute() -> SparseSlice:
        grids = slice_memo.get_or_compute(
            cache_key,
            lambda: _compute_slice_from_store(store, ts, variables, ocean_masked),
        )
        return _to_sparse_slice(store, grids)

    return _slice_dedup.dedupe(cache_key, compute)


def load_slice_uncached(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> SparseSlice:
    """The slice without L1, for animations, so they don't evict hot slices."""
    grids = _compute_slice_from_store(store, ts, variables, ocean_masked)
    return _to_sparse_slice(store, grids)
