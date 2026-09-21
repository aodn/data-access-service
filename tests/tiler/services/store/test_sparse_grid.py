"""SparseGrid: the CSR form of one variable's slice."""

import numpy as np

from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    index_dtype,
)
from tests.tiler.sparse_helpers import dense_of


def _dense(n_i=6, n_j=7, seed=0):
    rng = np.random.default_rng(seed)
    arr = rng.random((n_i, n_j)).astype(np.float32)
    arr[rng.random((n_i, n_j)) < 0.6] = np.nan
    arr[2] = np.nan  # an empty row
    return arr


def _from_dense(arr):
    i, j = np.nonzero(np.isfinite(arr))
    return SparseGrid.from_rows(
        i.astype(np.int16), j.astype(np.int16), arr[i, j], *arr.shape
    )


def test_the_whole_grid_round_trips():
    arr = _dense()
    np.testing.assert_array_equal(dense_of(_from_dense(arr)), arr)


def test_gather_picks_the_rows_and_cols_in_the_order_given():
    arr = _dense(20, 30)
    grid = _from_dense(arr)
    for rows, cols in [
        (np.arange(3, 9), np.arange(4, 17)),
        (np.array([2]), np.arange(30)),
        (np.array([7, 1, 7]), np.array([29, 0, 5])),
    ]:
        np.testing.assert_array_equal(grid.gather(rows, cols), arr[np.ix_(rows, cols)])


def test_from_rows_sorts_unsorted_rows():
    arr = _dense()
    i, j = np.nonzero(np.isfinite(arr))
    order = np.random.default_rng(1).permutation(i.size)
    grid = SparseGrid.from_rows(i[order], j[order], arr[i, j][order], *arr.shape)
    np.testing.assert_array_equal(dense_of(grid), arr)


def test_an_empty_grid_is_all_nan():
    empty = np.array([], dtype=np.int16)
    grid = SparseGrid.from_rows(empty, empty, np.array([], np.float32), 2, 3)
    assert np.isnan(dense_of(grid)).all()


def test_keep_drops_cells_outside_the_mask():
    arr = _dense(10, 12)
    valid = np.random.default_rng(2).random(arr.shape) < 0.5
    kept = _from_dense(arr).keep(valid)
    np.testing.assert_array_equal(dense_of(kept), np.where(valid, arr, np.nan))


def test_index_dtype_uses_int16_while_it_fits():
    assert index_dtype(32768) == np.int16
    assert index_dtype(32769) == np.int32


def test_value_at_returns_the_cell_or_nan():
    arr = _dense(10, 12)
    grid = _from_dense(arr)
    for i in range(10):
        for j in range(12):
            got = grid.value_at(i, j)
            if np.isnan(arr[i, j]):
                assert np.isnan(got)
            else:
                assert got == arr[i, j]


def test_min_and_max_are_worked_out_when_built():
    arr = _dense()
    grid = _from_dense(arr)
    assert grid.vmin == np.nanmin(arr)
    assert grid.vmax == np.nanmax(arr)


def test_min_and_max_of_an_empty_grid_are_nan():
    empty = np.array([], dtype=np.int16)
    grid = SparseGrid.from_rows(empty, empty, np.array([], np.float32), 1, 1)
    assert np.isnan(grid.vmin) and np.isnan(grid.vmax)


def test_keep_updates_min_and_max():
    arr = np.array([[1.0, 5.0]], dtype=np.float32)
    kept = _from_dense(arr).keep(np.array([[True, False]]))
    assert (kept.vmin, kept.vmax) == (1.0, 1.0)


def _block_nanmean(arr, rows, cols, out_r, out_c):
    """The same block means, done densely, to check ``aggregate`` against."""
    sub = arr[rows, cols]
    out_r = max(1, min(out_r, rows.stop - rows.start))
    out_c = max(1, min(out_c, cols.stop - cols.start))
    re = np.linspace(0, sub.shape[0], out_r + 1).astype(int)
    ce = np.linspace(0, sub.shape[1], out_c + 1).astype(int)
    out = np.full((out_r, out_c), np.nan, np.float32)
    for a in range(out_r):
        for b in range(out_c):
            block = sub[re[a] : re[a + 1], ce[b] : ce[b + 1]]
            if np.isfinite(block).any():
                out[a, b] = np.nanmean(block)
    return out


def test_aggregate_is_the_area_mean_of_every_cell():
    arr = _dense(40, 37, seed=3)
    grid = _from_dense(arr)
    for rows, cols, out_r, out_c in [
        (slice(0, 40), slice(0, 37), 5, 4),
        (slice(3, 33), slice(7, 30), 7, 6),
        (slice(0, 40), slice(0, 37), 1, 1),
    ]:
        got, _, _ = grid.aggregate(rows, cols, out_r, out_c)
        want = _block_nanmean(arr, rows, cols, out_r, out_c)
        np.testing.assert_allclose(got, want, rtol=1e-5, equal_nan=True)


def test_aggregate_never_asks_for_more_blocks_than_cells():
    arr = _dense(40, 37, seed=4)
    grid = _from_dense(arr)
    got, row_edges, col_edges = grid.aggregate(slice(10, 12), slice(5, 8), 50, 90)
    assert got.shape == (2, 3)
    np.testing.assert_array_equal(row_edges, [10, 11, 12])
    np.testing.assert_array_equal(col_edges, [5, 6, 7, 8])
    # One block per cell, so it is just the cells themselves.
    np.testing.assert_allclose(got, arr[10:12, 5:8], rtol=1e-5, equal_nan=True)


def test_aggregate_leaves_a_block_with_no_values_empty():
    arr = _dense(40, 37, seed=5)
    arr[:10] = np.nan  # a band with nothing in it
    grid = _from_dense(arr)
    got, _, _ = grid.aggregate(slice(0, 40), slice(0, 37), 4, 4)
    assert np.isnan(got[0]).all()
    assert np.isfinite(got[1:]).any()


def test_aggregate_edges_say_which_cells_each_block_covers():
    grid = _from_dense(_dense(12, 9, seed=6))
    _, row_edges, col_edges = grid.aggregate(slice(0, 12), slice(0, 9), 3, 3)
    np.testing.assert_array_equal(row_edges, [0, 4, 8, 12])
    np.testing.assert_array_equal(col_edges, [0, 3, 6, 9])
