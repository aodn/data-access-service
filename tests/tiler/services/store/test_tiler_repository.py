"""TilerParquetRepository: the read SQL for one variable's parquet file.

slice_loader's tests cover this indirectly through load_slice; these test the
repository directly — path resolution, NaN fill for cells the sparse rows
don't cover, and a missing file.
"""

import os

import duckdb
import numpy as np
import pytest

import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
)

TS = "2024-01-15T13:00:00.000000000Z"


@pytest.fixture(autouse=True)
def output_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(
        repo_module.Config.get_config(),
        "get_tiler_output_dir",
        lambda: str(tmp_path),
    )
    return tmp_path


@pytest.fixture
def session():
    with TilerDuckDBClient() as client:
        yield client


def _write_variable_parquet(output_dir, store, variable, ts, rows):
    path = variable_parquet_path(str(output_dir), store, variable, ts)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t (i INTEGER, j INTEGER, value FLOAT)")
    for row in rows:
        con.execute("INSERT INTO t VALUES (?, ?, ?)", row)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def test_fetch_variable_slice_fills_only_the_rows_present(output_dir, session):
    _write_variable_parquet(output_dir, "x", "v", TS, [(0, 0, 1.5)])
    repo = TilerParquetRepository(session)

    arr = repo.fetch_variable_slice("x", "v", TS, n_i=2, n_j=2, dtype="float32")

    assert arr.shape == (2, 2)
    assert arr.dtype == np.dtype("float32")
    assert arr[0, 0] == 1.5
    assert np.isnan(arr[0, 1])


def test_fetch_variable_slice_reads_the_file_for_that_timestamp(output_dir, session):
    other = "2024-01-15T14:00:00.000000000Z"
    _write_variable_parquet(output_dir, "x", "v", TS, [(0, 0, 1.0)])
    _write_variable_parquet(output_dir, "x", "v", other, [(0, 0, 2.0)])
    repo = TilerParquetRepository(session)

    arr = repo.fetch_variable_slice("x", "v", other, n_i=1, n_j=1, dtype="float32")

    assert arr[0, 0] == 2.0


def test_fetch_variable_slice_resolves_relative_to_output_dir(output_dir, session):
    _write_variable_parquet(output_dir, "foo", "v", TS, [(0, 0, 9.0)])
    repo = TilerParquetRepository(session)

    arr = repo.fetch_variable_slice("foo", "v", TS, n_i=1, n_j=1, dtype="float32")

    assert arr[0, 0] == 9.0


def test_fetch_variable_slice_of_an_empty_file_is_all_nan(output_dir, session):
    """A variable with no valid cells at an instant another variable of the
    same store does cover."""
    _write_variable_parquet(output_dir, "x", "v", TS, [])
    repo = TilerParquetRepository(session)

    arr = repo.fetch_variable_slice("x", "v", TS, n_i=1, n_j=2, dtype="float32")

    assert np.isnan(arr).all()


def test_fetch_variable_slice_raises_when_the_file_is_missing(output_dir, session):
    repo = TilerParquetRepository(session)

    with pytest.raises(FileNotFoundError, match="2024-01-16T000000"):
        repo.fetch_variable_slice(
            "x",
            "v",
            "2024-01-16T00:00:00.000000000Z",
            n_i=1,
            n_j=1,
            dtype="float32",
        )
