"""Unit tests for ParquetRepository.

The read methods (sites_in_date_range / latest_time / site_details) build SQL
and run it against the bound DuckDBSession. We exercise them against an
in-memory DuckDB seeded from a DataFrame, so no S3 / load() is needed. The
session's S3-secret creation is patched out (no AWS credentials in tests).
"""

import pandas as pd
import pytest

from data_access_service.models.duckdb_repository import (
    ParquetRepository,
    quote_ident,
)
from data_access_service.models.duckdb_session import DuckDBSession


class _GroupedRepo(ParquetRepository):
    """Mooring-shaped: groups its series by NOMINAL_DEPTH."""

    table = "test_mooring"
    bucket = "test-bucket"
    backup_bucket = "test-backup"
    dataset = "s3://test-bucket/test_mooring.parquet"
    backup_dataset = "s3://test-backup/test_mooring.parquet"
    time_column = "TIME"
    site_column = "site_code"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    value_columns = ("TEMP", "PSAL")
    group_column = "NOMINAL_DEPTH"


class _UngroupedRepo(ParquetRepository):
    """Wave-buoy-shaped: no group dimension."""

    table = "test_buoy"
    bucket = "test-bucket"
    backup_bucket = "test-backup"
    dataset = "s3://test-bucket/test_buoy.parquet"
    backup_dataset = "s3://test-backup/test_buoy.parquet"
    time_column = "TIME"
    site_column = "site_name"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    value_columns = ("WHTH",)


@pytest.fixture
def session(monkeypatch):
    # No AWS in tests, and these repos never call load(), so stub the S3 secret.
    monkeypatch.setattr(DuckDBSession, "create_s3_secret", lambda self, bucket: None)
    # extensions=() avoids any network (httpfs download) — not needed without load().
    s = DuckDBSession(database=":memory:", extensions=())
    yield s
    s.close()


def _materialize(repo: ParquetRepository, df: pd.DataFrame) -> None:
    """Seed the repo's table from a DataFrame, standing in for load()."""
    conn = repo.session.conn
    conn.register("_seed_df", df)
    conn.execute(
        f"CREATE OR REPLACE TABLE {quote_ident(repo.table)} AS SELECT * FROM _seed_df"
    )
    conn.unregister("_seed_df")


@pytest.fixture
def grouped_rows():
    return pd.DataFrame(
        {
            "site_code": ["A", "A", "A", "B"],
            "TIME": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"]
            ),
            "LATITUDE": [-30.0, -30.0, -30.0, -32.0],
            "LONGITUDE": [150.0, 150.0, 150.0, 151.0],
            "NOMINAL_DEPTH": [10, 10, 20, 10],
            "TEMP": [20.0, 21.0, 18.0, 19.0],
            "PSAL": [35.0, None, 34.0, 33.0],
        }
    )


# --- quote_ident -------------------------------------------------------------


def test_quote_ident_quotes_and_escapes():
    assert quote_ident("site_code") == '"site_code"'
    assert quote_ident('a"b') == '"a""b"'


# --- subclass contract -------------------------------------------------------


def test_subclass_missing_required_attr_raises_typeerror():
    with pytest.raises(TypeError) as excinfo:
        # missing bucket / dataset / time_column / ... -> fails at class creation
        class _Bad(ParquetRepository):
            table = "x"

    assert "must define class attributes" in str(excinfo.value)


def test_group_column_is_optional():
    # _UngroupedRepo never sets group_column; it should default to None, not raise.
    assert _UngroupedRepo.group_column is None


def test_abstract_base_cannot_be_instantiated(session):
    with pytest.raises(TypeError):
        ParquetRepository(session)


# --- load_columns ------------------------------------------------------------


def test_load_columns_grouped_includes_group_no_dupes(session):
    repo = _GroupedRepo(session)
    assert repo.load_columns == [
        "TIME",
        "site_code",
        "LATITUDE",
        "LONGITUDE",
        "NOMINAL_DEPTH",
        "TEMP",
        "PSAL",
    ]


def test_load_columns_ungrouped_omits_group(session):
    repo = _UngroupedRepo(session)
    assert repo.load_columns == ["TIME", "site_name", "LATITUDE", "LONGITUDE", "WHTH"]


# --- is_loaded ---------------------------------------------------------------


def test_is_loaded_reflects_table_presence(session, grouped_rows):
    repo = _GroupedRepo(session)
    assert repo.is_loaded() is False
    _materialize(repo, grouped_rows)
    assert repo.is_loaded() is True


# --- sites_in_date_range -----------------------------------------------------


def test_sites_in_date_range_one_row_per_site_latest_location(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)

    out = repo.sites_in_date_range()
    assert list(out["site_code"]) == ["A", "B"]  # ordered by site

    a = out[out["site_code"] == "A"].iloc[0]
    # arg_max picks lat/lon from each site's most recent record
    assert pd.Timestamp(a["time"]) == pd.Timestamp("2024-01-02")
    assert a["latitude"] == -30.0
    assert a["longitude"] == 150.0


def test_sites_in_date_range_start_filter(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    out = repo.sites_in_date_range(start="2024-01-03")
    assert list(out["site_code"]) == ["B"]


def test_sites_in_date_range_end_filter(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    out = repo.sites_in_date_range(end="2024-01-01")
    # only records on/before 2024-01-01 -> site A only
    assert list(out["site_code"]) == ["A"]


# --- latest_time -------------------------------------------------------------


def test_latest_time(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    assert pd.Timestamp(repo.latest_time()) == pd.Timestamp("2024-01-03")


# --- site_details ------------------------------------------------------------


def test_site_details_grouped_ordered_by_group_then_time(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    rows = repo.site_details("A")

    # selects required + value columns (site column is filtered on, not returned)
    assert {"TIME", "LATITUDE", "LONGITUDE", "NOMINAL_DEPTH", "TEMP", "PSAL"}.issubset(
        rows.columns
    )
    # site A: depth 10 at two times, depth 20 at one -> ordered by depth then time
    assert list(rows["NOMINAL_DEPTH"]) == [10, 10, 20]
    assert [pd.Timestamp(t) for t in rows["TIME"]] == [
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-01"),
    ]


def test_site_details_time_filter(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    rows = repo.site_details("A", start="2024-01-02")
    assert len(rows) == 1
    assert pd.Timestamp(rows.iloc[0]["TIME"]) == pd.Timestamp("2024-01-02")


def test_site_details_unknown_site_is_empty(session, grouped_rows):
    repo = _GroupedRepo(session)
    _materialize(repo, grouped_rows)
    rows = repo.site_details("does-not-exist")
    assert len(rows) == 0
