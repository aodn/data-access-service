"""Convert between dense test data and the CSR slice (``SparseSlice``)."""

import numpy as np
import xarray as xr

from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    SparseSlice,
)


def sparse_of(ds: xr.Dataset) -> SparseSlice:
    grids = {}
    for v in ds.data_vars:
        arr = ds[v].values
        i, j = np.nonzero(np.isfinite(arr))
        grids[v] = SparseGrid.from_rows(i, j, arr[i, j], *arr.shape)
    return SparseSlice(
        lat=ds.lat.values,
        lon=ds.lon.values,
        grids=grids,
        attrs={v: dict(ds[v].attrs) for v in ds.data_vars},
    )


def dense_of(grid: SparseGrid) -> np.ndarray:
    return grid.gather(np.arange(grid.n_i), np.arange(grid.n_j))
