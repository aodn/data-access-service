"""Unit tests for TilerBatchDuckDBClient (the batch conversion's writer)."""

import os

import pandas as pd
import pytest

from data_access_service.core.duckdbclient import TilerBatchDuckDBClient
from data_access_service.models.tiler_types import TilerBatchDuckDBConfig

CONFIG = TilerBatchDuckDBConfig(
    memory_limit="128MB", threads=1, duckdb_temp_dir="test_tiler_batch_"
)


def test_spill_directory_is_applied_and_removed_on_close():
    with TilerBatchDuckDBClient(CONFIG) as client:
        (temp_dir,) = client.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()
        assert os.path.basename(temp_dir).startswith("test_tiler_batch_")
        assert os.path.isdir(temp_dir)
    assert not os.path.exists(temp_dir)


def test_write_parquet_round_trips(tmp_path):
    path = str(tmp_path / "out.parquet")
    with TilerBatchDuckDBClient(CONFIG) as client:
        client.write_parquet(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}), path)
        rows = client.execute(
            f"SELECT a, b FROM read_parquet('{path}') ORDER BY a"
        ).fetchall()
    assert rows == [(1, "x"), (2, "y")]


def test_merge_parquet_keeps_file_and_row_order(tmp_path):
    config = TilerBatchDuckDBConfig(
        memory_limit="128MB", threads=4, duckdb_temp_dir="test_tiler_batch_"
    )
    paths = []
    with TilerBatchDuckDBClient(config) as client:
        # Several row groups per file, so threads could reorder them.
        for n in range(3):
            path = str(tmp_path / f"piece_{n}.parquet")
            client.write_parquet(
                pd.DataFrame({"a": range(n * 300_000, (n + 1) * 300_000)}), path
            )
            paths.append(path)
        out = str(tmp_path / "out.parquet")
        client.merge_parquet(paths, out)

    assert list(pd.read_parquet(out)["a"]) == list(range(900_000))


def test_execute_binds_params():
    with TilerBatchDuckDBClient(CONFIG) as client:
        (value,) = client.execute("SELECT ? + ?", [1, 2]).fetchone()
        assert value == 3


def test_closed_client_raises():
    client = TilerBatchDuckDBClient(CONFIG)
    client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.execute("SELECT 1")
    client.close()  # safe to call twice
