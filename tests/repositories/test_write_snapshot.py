"""Unit tests for ParquetRepository.write_snapshot()'s row ordering.

write_snapshot() sorts by (site_column [, group_column], time_column) before
writing so the snapshot's Parquet row-group min/max stats prune well for
site_details()'s per-site queries (see the docstring on write_snapshot()).
These tests exercise the real method end to end against a local Parquet file.
"""

import pandas as pd
import pyarrow.parquet as pq
import pytest

from data_access_service.core.duckdbclient import DuckDBClient, SitesDuckDBClient
from data_access_service.sites.sites_repository import ParquetRepository, quote_ident


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    s = SitesDuckDBClient()
    yield s
    s.close()


def _make_repo(session, tmp_path, *, table_name, group_column=None):
    attrs = {
        "table": table_name,
        "bucket": "test-bucket",
        "snapshot_bucket": "test-snapshot",
        "dataset": f"s3://test-bucket/{table_name}.parquet",
        "snapshot_dataset": str(tmp_path / f"{table_name}.parquet"),
        "time_column": "TIME",
        "site_column": "site_code",
        "latitude_column": "LATITUDE",
        "longitude_column": "LONGITUDE",
        "value_columns": ("TEMP",),
    }
    if group_column is not None:
        attrs["group_column"] = group_column
    cls = type("_WriteSnapshotRepo", (ParquetRepository,), attrs)
    return cls(session)


def _seed_table(repo: ParquetRepository, df: pd.DataFrame) -> None:
    conn = repo.session.get_instance()
    conn.register("_seed_df", df)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {quote_ident(repo.table)} AS SELECT * FROM _seed_df"
        )
    finally:
        conn.unregister("_seed_df")


def test_write_snapshot_sorts_by_site_then_time(session, tmp_path):
    repo = _make_repo(session, tmp_path, table_name="test_write_snapshot_sites")
    df = pd.DataFrame(
        {
            "TIME": pd.to_datetime(
                ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-01"]
            ),
            "site_code": ["B", "A", "A", "B"],
            "LATITUDE": [-30.0, -31.0, -31.0, -30.0],
            "LONGITUDE": [150.0, 151.0, 151.0, 150.0],
            "TEMP": [1.0, 2.0, 3.0, 4.0],
        }
    )
    _seed_table(repo, df)

    repo.write_snapshot()

    written = pq.read_table(repo.snapshot_dataset).to_pandas()
    assert list(written["site_code"]) == ["A", "A", "B", "B"]
    assert list(written["TIME"]) == list(
        pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"])
    )


def test_write_snapshot_sorts_by_site_then_group_then_time(session, tmp_path):
    repo = _make_repo(
        session,
        tmp_path,
        table_name="test_write_snapshot_grouped",
        group_column="NOMINAL_DEPTH",
    )
    df = pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "site_code": ["A", "A", "A"],
            "NOMINAL_DEPTH": [10, 5, 5],
            "LATITUDE": [-31.0, -31.0, -31.0],
            "LONGITUDE": [151.0, 151.0, 151.0],
            "TEMP": [1.0, 2.0, 3.0],
        }
    )
    _seed_table(repo, df)

    repo.write_snapshot()

    written = pq.read_table(repo.snapshot_dataset).to_pandas()
    assert list(written["NOMINAL_DEPTH"]) == [5, 5, 10]


