"""Open a (store, timestamp, variables) slice. No values are read here:
each grid is a ``ParquetGridSource``, and the renderers ask DuckDB for only
what they need."""

import numpy as np
import pandas as pd

from data_access_service.tiler.services.rendering.masks import ocean_valid_for_coords
from data_access_service.tiler.services.store.parquet_grid_source import (
    ParquetGridSource,
)
from data_access_service.tiler.services.store.registry import (
    get_store_metadata,
    resolve_timestamp,
    unavailable_date_message,
)
from data_access_service.tiler.services.store.sparse_grid import SparseSlice
from data_access_service.tiler.services.store.tiler_repository import cell_table


def _ocean_table(store: str) -> str:
    """The table of the store's valid-ocean cells, built once per grid."""
    meta = get_store_metadata(store)
    # A refresh that changed the grid gets a new table.
    grid = (meta.n_i, meta.n_j, meta.lat[0], meta.lat[-1], meta.lon[0], meta.lon[-1])
    key = ("ocean", store, grid)

    def cells() -> tuple[np.ndarray, np.ndarray]:
        return np.nonzero(ocean_valid_for_coords(meta.lon, meta.lat))

    return cell_table(key, cells)


def load_slice(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
) -> SparseSlice:
    """The slice at exact instant ``ts``. Raises FileNotFoundError for an
    unknown variable or date, or a missing file."""
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

    keep = _ocean_table(store) if ocean_masked else None
    grids = {
        v: ParquetGridSource(
            store,
            v,
            raw_ts,
            meta.n_i,
            meta.n_j,
            np.dtype(meta.variables[v].dtype),
            keep,
        )
        for v in variables
    }
    # Reading the range here makes a missing file a 404 now rather than an
    # error mid-render; nearly every renderer needs it anyway, and it's cached.
    for grid in grids.values():
        _ = grid.vmin
    return SparseSlice(
        lat=np.asarray(meta.lat),
        lon=np.asarray(meta.lon),
        grids=grids,
        attrs={v: dict(meta.variables[v].attrs) for v in variables},
    )
