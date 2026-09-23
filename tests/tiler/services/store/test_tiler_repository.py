"""TilerParquetRepository: the read SQL for one variable's parquet file.

slice_loader's tests cover this through load_slice; these test each query
directly, and check the SQL answers against ``SparseGrid``, which does the
same reads in memory.
"""

import os

import duckdb
import numpy as np
import pyarrow as pa
import pytest

import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path
from data_access_service.tiler.services.rendering.kernels import _block_edges
from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    block_means,
    window_edges,
)
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
)

TS = "2024-01-15T13:00:00.000000000Z"


@pytest.fixture(autouse=True)
def tiler_root_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(
        repo_module.Config.get_config(),
        "get_tiler_root_dir",
        lambda: str(tmp_path),
    )
    return tmp_path


@pytest.fixture
def repo():
    with TilerDuckDBClient() as client:
        yield TilerParquetRepository(client)


def _write(tiler_root_dir, rows, variable="v", ts=TS, store="x", **copy):
    """Write ``(i, j, value)`` rows as batch does."""
    path = variable_parquet_path(str(tiler_root_dir), store, variable, ts)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    i, j, value = (np.array(c) for c in zip(*rows)) if rows else ([], [], [])
    table = pa.table(
        {
            "i": np.asarray(i, np.int32),
            "j": np.asarray(j, np.int32),
            "value": np.asarray(value, np.float32),
        }
    )
    options = ", ".join(["FORMAT PARQUET", *(f"{k} {v}" for k, v in copy.items())])
    con = duckdb.connect(":memory:")
    con.register("t", table)
    con.execute(f"COPY t TO '{path}' ({options})")
    con.close()


def _random_grid(n_i=60, n_j=80, seed=0):
    """A sparse float32 grid, and its rows in batch order."""
    rng = np.random.default_rng(seed)
    arr = (rng.normal(size=(n_i, n_j)) * 3 + 10).astype(np.float32)
    arr[rng.random((n_i, n_j)) < 0.5] = np.nan
    arr[5] = np.nan  # an empty row
    i, j = np.nonzero(np.isfinite(arr))
    return arr, list(zip(i, j, arr[i, j]))


def _grid_of(arr):
    i, j = np.nonzero(np.isfinite(arr))
    return SparseGrid.from_rows(i, j, arr[i, j], *arr.shape)


# --- value range ---


def test_value_range_is_the_min_and_max(tiler_root_dir, repo):
    arr, rows = _random_grid()
    _write(tiler_root_dir, rows, ROW_GROUP_SIZE=500)

    lo, hi = repo.fetch_value_range("x", "v", TS)

    # Exactly what the in-memory grid works out, not merely close: the
    # manifest sends these and data tiles are normalised with them.
    grid = _grid_of(arr)
    assert (lo, hi) == (grid.vmin, grid.vmax)


def test_value_range_of_an_empty_file_is_nan(tiler_root_dir, repo):
    _write(tiler_root_dir, [])

    lo, hi = repo.fetch_value_range("x", "v", TS)

    assert np.isnan(lo) and np.isnan(hi)


def test_value_range_counts_only_the_kept_cells(tiler_root_dir, repo):
    _write(tiler_root_dir, [(0, 0, 1.0), (0, 1, 50.0), (1, 1, 3.0)])
    repo.create_cell_table("keep", np.array([0, 1]), np.array([0, 1]))

    assert repo.fetch_value_range("x", "v", TS, keep="keep") == (1.0, 3.0)


def test_value_range_reads_the_file_for_that_timestamp(tiler_root_dir, repo):
    other = "2024-01-15T14:00:00.000000000Z"
    _write(tiler_root_dir, [(0, 0, 1.0)])
    _write(tiler_root_dir, [(0, 0, 2.0)], ts=other)

    assert repo.fetch_value_range("x", "v", other) == (2.0, 2.0)


# --- point ---


def test_point_is_the_cell_or_nan(tiler_root_dir, repo):
    _write(tiler_root_dir, [(0, 0, 1.5), (1, 2, 4.0)])

    assert repo.fetch_point("x", "v", TS, 1, 2) == 4.0
    assert np.isnan(repo.fetch_point("x", "v", TS, 0, 1))


def test_point_outside_the_kept_cells_is_nan(tiler_root_dir, repo):
    _write(tiler_root_dir, [(0, 0, 1.5), (1, 2, 4.0)])
    repo.create_cell_table("keep", np.array([0]), np.array([0]))

    assert repo.fetch_point("x", "v", TS, 0, 0, keep="keep") == 1.5
    assert np.isnan(repo.fetch_point("x", "v", TS, 1, 2, keep="keep"))


# --- gather ---


def test_gather_matches_the_in_memory_grid(tiler_root_dir, repo):
    arr, rows = _random_grid()
    _write(tiler_root_dir, rows, ROW_GROUP_SIZE=500)
    grid = _grid_of(arr)

    for r, c in [
        (np.arange(3, 29), np.arange(4, 57)),
        (np.array([5]), np.arange(80)),  # the empty row
        (np.array([40, 1, 17]), np.array([79, 0, 33])),  # any order
        (np.arange(0, 60, 7), np.arange(0, 80, 9)),  # sampled
    ]:
        got = repo.fetch_gather("x", "v", TS, r, c, np.dtype("float32"))
        np.testing.assert_array_equal(got, grid.gather(r, c))
        assert got.dtype == np.float32


def test_gather_of_nothing_reads_nothing(tiler_root_dir, repo):
    # No file: an empty request must not touch it.
    got = repo.fetch_gather(
        "x", "v", TS, np.array([], int), np.arange(3), np.dtype("float32")
    )
    assert got.shape == (0, 3)


def test_gather_leaves_cells_outside_the_kept_ones_nan(tiler_root_dir, repo):
    _write(tiler_root_dir, [(0, 0, 1.0), (0, 1, 2.0)])
    repo.create_cell_table("keep", np.array([0]), np.array([1]))

    got = repo.fetch_gather(
        "x", "v", TS, np.array([0]), np.array([0, 1]), np.dtype("float32"), "keep"
    )

    np.testing.assert_array_equal(got, [[np.nan, 2.0]])


# --- block sums ---


def test_block_means_match_the_in_memory_grid(tiler_root_dir, repo):
    arr, rows = _random_grid()
    _write(tiler_root_dir, rows, ROW_GROUP_SIZE=500)
    grid = _grid_of(arr)

    cases = [
        # The visual path's window edges.
        window_edges(slice(0, 60), slice(0, 80), 7, 9),
        window_edges(slice(10, 43), slice(21, 70), 5, 30),
        # The data-tile path's edges, which ignore the window.
        (_block_edges(60, 23, 3, 17), _block_edges(80, 31, 0, 31)),
    ]
    for row_edges, col_edges in cases:
        want, _, _ = grid.aggregate_blocks(row_edges, col_edges)
        got = block_means(*repo.fetch_block_sums("x", "v", TS, row_edges, col_edges))
        np.testing.assert_array_equal(got, want)


def test_block_sums_count_only_the_kept_cells(tiler_root_dir, repo):
    _write(tiler_root_dir, [(0, 0, 1.0), (0, 1, 3.0), (1, 0, 100.0)])
    repo.create_cell_table("keep", np.array([0, 0]), np.array([0, 1]))

    total, count = repo.fetch_block_sums(
        "x", "v", TS, np.array([0, 2]), np.array([0, 2]), keep="keep"
    )

    assert (total[0, 0], count[0, 0]) == (4.0, 2)


def test_block_sums_of_an_empty_window_read_nothing(tiler_root_dir, repo):
    total, count = repo.fetch_block_sums(
        "x", "v", TS, np.array([3, 3]), np.array([0, 4])
    )
    assert total.shape == (1, 1) and count[0, 0] == 0


# --- missing file ---


@pytest.mark.parametrize(
    "read",
    [
        lambda repo, ts: repo.fetch_value_range("x", "v", ts),
        lambda repo, ts: repo.fetch_point("x", "v", ts, 0, 0),
        lambda repo, ts: repo.fetch_gather(
            "x", "v", ts, np.array([0]), np.array([0]), np.dtype("float32")
        ),
        lambda repo, ts: repo.fetch_block_sums(
            "x", "v", ts, np.array([0, 1]), np.array([0, 1])
        ),
    ],
    ids=["range", "point", "gather", "block_sums"],
)
def test_a_missing_file_raises_file_not_found(tiler_root_dir, repo, read):
    with pytest.raises(FileNotFoundError, match="2024-01-16T000000"):
        read(repo, "2024-01-16T00:00:00.000000000Z")
