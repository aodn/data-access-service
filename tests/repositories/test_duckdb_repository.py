"""Unit tests for ParquetRepository.

The read methods (sites_in_date_range / latest_time / site_details) build SQL
and run it against the bound ParquetDuckDBClient. We exercise them against an
in-memory DuckDB seeded from a DataFrame, so no S3 / load() is needed. The
session's S3-secret creation is patched out (no AWS credentials in tests).
"""

import pandas as pd
import pytest

from data_access_service.sites.sites_repository import (
    ParquetRepository,
    quote_ident,
)
from data_access_service.core.duckdbclient import ParquetDuckDBClient


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


class _QcRepo(ParquetRepository):
    """Grouped, with both value columns QC-gated."""

    table = "test_qc"
    bucket = "test-bucket"
    backup_bucket = "test-backup"
    dataset = "s3://test-bucket/test_qc.parquet"
    backup_dataset = "s3://test-backup/test_qc.parquet"
    time_column = "TIME"
    site_column = "site_code"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    group_column = "NOMINAL_DEPTH"
    value_columns = ("TEMP", "PSAL")
    value_columns_quality_control_columns = (
        "TEMP_quality_control",
        "PSAL_quality_control",
    )


@pytest.fixture
def session(monkeypatch):
    # No AWS in tests, and these repos never call load(), so stub the S3 secret.
    monkeypatch.setattr(
        ParquetDuckDBClient, "create_s3_secret", lambda self, bucket: None
    )
    # The autouse memory_parquets_config fixture keeps this in-memory + offline.
    s = ParquetDuckDBClient()
    yield s
    s.close()


def _materialize(repo: ParquetRepository, df: pd.DataFrame) -> None:
    """Seed the repo's table from a DataFrame, standing in for load()."""
    conn = repo.session.get_instance()
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


# --- value_columns_quality_control_columns -----------------------------------


def test_qc_columns_default_to_empty():
    assert _GroupedRepo.value_columns_quality_control_columns == ()
    assert _UngroupedRepo.value_columns_quality_control_columns == ()


def test_qc_columns_length_must_match_value_columns_or_be_empty():
    with pytest.raises(TypeError) as excinfo:

        class _MismatchedQcRepo(ParquetRepository):
            table = "x"
            bucket = "test-bucket"
            backup_bucket = "test-backup"
            dataset = "s3://test-bucket/x.parquet"
            backup_dataset = "s3://test-backup/x.parquet"
            time_column = "TIME"
            site_column = "site_code"
            latitude_column = "LATITUDE"
            longitude_column = "LONGITUDE"
            value_columns = ("TEMP", "PSAL")
            value_columns_quality_control_columns = ("TEMP_quality_control",)

    assert "value_columns_quality_control_columns" in str(excinfo.value)


def test_load_columns_includes_qc_columns(session):
    repo = _QcRepo(session)
    assert repo.load_columns == [
        "TIME",
        "site_code",
        "LATITUDE",
        "LONGITUDE",
        "NOMINAL_DEPTH",
        "TEMP",
        "PSAL",
        "TEMP_quality_control",
        "PSAL_quality_control",
    ]


@pytest.fixture
def qc_rows():
    return pd.DataFrame(
        {
            "site_code": ["A", "A", "A"],
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "LATITUDE": [-30.0, -30.0, -30.0],
            "LONGITUDE": [150.0, 150.0, 150.0],
            "NOMINAL_DEPTH": [10, 10, 10],
            "TEMP": [20.0, 21.0, 22.0],
            "PSAL": [35.0, 34.0, 33.0],
            "TEMP_quality_control": [1, 4, 1],  # 4 = bad -> TEMP nulled on row 2
            "PSAL_quality_control": [1, 1, 9],  # 9 = bad -> PSAL nulled on row 3
        }
    )


def test_site_details_nulls_bad_quality_values_only(session, qc_rows):
    repo = _QcRepo(session)
    _materialize(repo, qc_rows)
    rows = repo.site_details("A")

    # good quality values pass through unchanged; bad ones become NaN
    assert list(rows["TEMP"].isna()) == [False, True, False]
    assert list(rows["PSAL"].isna()) == [False, False, True]
    assert rows.iloc[0]["TEMP"] == 20.0
    assert rows.iloc[2]["TEMP"] == 22.0
    # only the failing value in a row is nulled; its sibling column is untouched
    assert rows.iloc[1]["PSAL"] == 34.0
    # QC columns themselves aren't exposed in the output
    assert "TEMP_quality_control" not in rows.columns
    assert "PSAL_quality_control" not in rows.columns
