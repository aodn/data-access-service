"""TilerParquetRepository: the read SQL bound to one store's parquet directory.

slice_loader's tests cover this indirectly through load_slice; these test the
repository directly — path construction, the exact-timestamp filter, and NaN
fill for cells the sparse rows don't cover.
"""

from unittest.mock import MagicMock

import duckdb
import numpy as np
import pytest

import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
)


@pytest.fixture(autouse=True)
def output_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(
        repo_module.Config.get_config(),
        "get_tiler_parquet_config",
        lambda: MagicMock(output_dir=str(tmp_path)),
    )
    return tmp_path


@pytest.fixture
def session():
    with TilerDuckDBClient() as client:
        yield client


def _write_variable_parquet(output_dir, dataset_stem, variable, rows):
    path = output_dir / dataset_stem / f"{variable}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (timestamp VARCHAR, i INTEGER, j INTEGER, value FLOAT)"
    )
    for row in rows:
        con.execute("INSERT INTO t VALUES (?, ?, ?, ?)", row)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def test_fetch_variable_slice_fills_only_the_rows_present(output_dir, session):
    _write_variable_parquet(
        output_dir, "x", "v", [("2024-01-15T13:00:00.000000000Z", 0, 0, 1.5)]
    )
    repo = TilerParquetRepository(session, "x")

    arr = repo.fetch_variable_slice(
        "v", "2024-01-15T13:00:00.000000000Z", n_i=2, n_j=2, dtype="float32"
    )

    assert arr.shape == (2, 2)
    assert arr.dtype == np.dtype("float32")
    assert arr[0, 0] == 1.5
    assert np.isnan(arr[0, 1])


def test_fetch_variable_slice_filters_to_the_exact_timestamp(output_dir, session):
    _write_variable_parquet(
        output_dir,
        "x",
        "v",
        [
            ("2024-01-15T13:00:00.000000000Z", 0, 0, 1.0),
            ("2024-01-15T14:00:00.000000000Z", 0, 0, 2.0),
        ],
    )
    repo = TilerParquetRepository(session, "x")

    arr = repo.fetch_variable_slice(
        "v", "2024-01-15T14:00:00.000000000Z", n_i=1, n_j=1, dtype="float32"
    )

    assert arr[0, 0] == 2.0


def test_fetch_variable_slice_keyed_by_dataset_stem_not_full_url(output_dir, session):
    _write_variable_parquet(
        output_dir, "foo", "v", [("2024-01-15T13:00:00.000000000Z", 0, 0, 9.0)]
    )
    repo = TilerParquetRepository(session, "foo")

    arr = repo.fetch_variable_slice(
        "v", "2024-01-15T13:00:00.000000000Z", n_i=1, n_j=1, dtype="float32"
    )

    assert arr[0, 0] == 9.0


def test_fetch_variable_slice_raises_when_timestamp_has_zero_rows(output_dir, session):
    """A timestamp the store's full history lists (metadata.json) but a
    partial/sampled batch run never converted to parquet — must 404, not
    silently return an all-NaN slice (which broke manifest.json's valueRange:
    NaN -> json_safe_float -> None -> pydantic ValidationError -> 500)."""
    _write_variable_parquet(
        output_dir, "x", "v", [("2024-01-15T13:00:00.000000000Z", 0, 0, 1.0)]
    )
    repo = TilerParquetRepository(session, "x")

    with pytest.raises(FileNotFoundError, match="2024-01-16T00:00:00"):
        repo.fetch_variable_slice(
            "v", "2024-01-16T00:00:00.000000000Z", n_i=1, n_j=1, dtype="float32"
        )
