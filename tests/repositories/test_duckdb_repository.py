"""Unit tests for ParquetRepository.

The read methods (sites_in_date_range / latest_time / site_details) build SQL
and run it against the bound ParquetDuckDBClient. We exercise them against an
in-memory DuckDB seeded from a DataFrame, so no S3 / load() is needed. The
session's S3-secret creation is patched out (no AWS credentials in tests).
"""

import os

import pandas as pd
import pytest

from data_access_service.sites.sites_repository import (
    ParquetRepository,
    quote_ident,
)
from data_access_service.core.duckdbclient import ParquetDuckDBClient, DuckDBClient


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
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
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


# --- load_incremental ---------------------------------------------------------


class _IncrementalRepo(ParquetRepository):
    """For load_incremental() tests. ``dataset`` is set per-test to a local
    tmp dir standing in for the S3 prefix — read_parquet globs local paths the
    same way it globs s3:// ones. No Hive partitioning here (flat files) —
    this exercises the fallback path for a dataset with no partition_column;
    see _HivePartitionedIncrementalRepo below for the coarse-pruning path."""

    table = "test_incremental"
    bucket = "test-bucket"
    backup_bucket = "test-backup"
    dataset = ""
    backup_dataset = "s3://test-backup/test_incremental.parquet"
    time_column = "TIME"
    site_column = "site_code"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    value_columns = ("TEMP",)
    incremental_lookback_days = 5
    partition_column = None


class _HivePartitionedIncrementalRepo(_IncrementalRepo):
    """Same as _IncrementalRepo, but with the real repos' default
    partition_column="timestamp" — files live under timestamp=<epoch>/."""

    table = "test_incremental_hive"
    partition_column = "timestamp"


def _write_file(con, root: str, name: str, rows: pd.DataFrame) -> None:
    con.register("_seed_df", rows)
    con.execute(
        f"COPY (SELECT * FROM _seed_df) TO '{os.path.join(root, name)}' (FORMAT PARQUET)"
    )
    con.unregister("_seed_df")


def _rows(site: str, time: pd.Timestamp, temp: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "site_code": [site],
            "TIME": [time],
            "LATITUDE": [-30.0],
            "LONGITUDE": [150.0],
            "TEMP": [temp],
        }
    )


@pytest.fixture
def incremental_repo(session, tmp_path):
    repo = _IncrementalRepo(session)
    repo.dataset = str(tmp_path)
    return repo


def test_incremental_cutoff_matches_lookback_days(session):
    repo = _IncrementalRepo(session)
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    cutoff = repo._incremental_cutoff(5)
    assert abs((now - pd.Timedelta(days=5) - cutoff).total_seconds()) < 1


def test_load_incremental_only_touches_recent_rows(incremental_repo):
    con = incremental_repo.session.get_instance()
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    old_time = now - pd.Timedelta(days=30)  # outside default lookback=5 days
    recent_time = now - pd.Timedelta(days=1)  # inside lookback

    _write_file(
        con, incremental_repo.dataset, "old.parquet", _rows("A", old_time, 10.0)
    )
    _write_file(
        con, incremental_repo.dataset, "recent.parquet", _rows("B", recent_time, 20.0)
    )

    incremental_repo.load()
    loaded = con.execute(
        f"SELECT * FROM {quote_ident(incremental_repo.table)} ORDER BY site_code"
    ).df()
    assert list(loaded["site_code"]) == ["A", "B"]

    # Overwrite the recent file as if new/corrected data arrived upstream;
    # leave the old (out-of-lookback) file untouched.
    _write_file(
        con, incremental_repo.dataset, "recent.parquet", _rows("B", recent_time, 999.0)
    )

    incremental_repo.load_incremental()  # default lookback_days=5
    refreshed = con.execute(
        f"SELECT * FROM {quote_ident(incremental_repo.table)} ORDER BY site_code"
    ).df()

    assert list(refreshed["site_code"]) == ["A", "B"]
    # Old row: untouched by the incremental refresh.
    assert refreshed.loc[refreshed["site_code"] == "A", "TEMP"].item() == 10.0
    # Recent row: replaced with the new value.
    assert refreshed.loc[refreshed["site_code"] == "B", "TEMP"].item() == 999.0


def _write_hive_partition(
    con, root: str, partition_start: pd.Timestamp, rows: pd.DataFrame
) -> None:
    """Write one timestamp=<epoch>/ Hive partition folder, matching how the
    real mooring/wave-buoy datasets are laid out (see MooringRepository)."""
    epoch = int(partition_start.tz_localize("UTC").timestamp())
    part_dir = os.path.join(root, f"timestamp={epoch}")
    os.makedirs(part_dir, exist_ok=True)
    _write_file(con, part_dir, "data.parquet", rows)


@pytest.fixture
def hive_incremental_repo(session, tmp_path):
    repo = _HivePartitionedIncrementalRepo(session)
    repo.dataset = str(tmp_path)
    return repo


def test_load_incremental_with_partition_column_still_correct(hive_incremental_repo):
    """Same correctness contract as the flat-file case, just laid out under
    timestamp=<epoch>/ so the coarse partition_column pre-filter applies."""
    con = hive_incremental_repo.session.get_instance()
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    old_start = now - pd.Timedelta(days=30)  # outside default lookback=5 days
    recent_start = now - pd.Timedelta(days=1)  # inside lookback

    _write_hive_partition(
        con, hive_incremental_repo.dataset, old_start, _rows("A", old_start, 10.0)
    )
    _write_hive_partition(
        con, hive_incremental_repo.dataset, recent_start, _rows("B", recent_start, 20.0)
    )

    hive_incremental_repo.load()

    # Overwrite the recent partition as if new/corrected data arrived
    # upstream; leave the old (out-of-lookback) partition untouched.
    _write_hive_partition(
        con,
        hive_incremental_repo.dataset,
        recent_start,
        _rows("B", recent_start, 999.0),
    )

    hive_incremental_repo.load_incremental()  # default lookback_days=5
    refreshed = con.execute(
        f"SELECT * FROM {quote_ident(hive_incremental_repo.table)} ORDER BY site_code"
    ).df()

    assert list(refreshed["site_code"]) == ["A", "B"]
    assert refreshed.loc[refreshed["site_code"] == "A", "TEMP"].item() == 10.0
    assert refreshed.loc[refreshed["site_code"] == "B", "TEMP"].item() == 999.0


def test_qualifying_partition_globs_picks_exact_boundary(hive_incremental_repo):
    con = hive_incremental_repo.session.get_instance()
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    very_old_start = now - pd.Timedelta(days=1000)
    recent_start = now - pd.Timedelta(days=1)

    _write_hive_partition(
        con,
        hive_incremental_repo.dataset,
        very_old_start,
        _rows("A", very_old_start, 10.0),
    )
    _write_hive_partition(
        con, hive_incremental_repo.dataset, recent_start, _rows("B", recent_start, 20.0)
    )

    recent_epoch = int(recent_start.tz_localize("UTC").timestamp())
    very_old_epoch = int(very_old_start.tz_localize("UTC").timestamp())

    # cutoff == now: only the recent partition is at-or-before it, and
    # nothing exists after it, so only its glob is returned.
    cutoff_epoch = int(now.tz_localize("UTC").timestamp())
    globs = hive_incremental_repo._qualifying_partition_globs(cutoff_epoch)
    assert len(globs) == 1
    assert f"timestamp={recent_epoch}" in globs[0]
    assert globs[0].endswith("/**/*.parquet")
    assert globs[0].count("**") == 1

    # Cutoff before every existing partition: falls back to the earliest one
    # (nothing to safely exclude) instead of finding no match — so BOTH
    # partitions qualify (earliest onward).
    globs = hive_incremental_repo._qualifying_partition_globs(very_old_epoch - 1)
    assert len(globs) == 2
    assert any(f"timestamp={very_old_epoch}" in g for g in globs)
    assert any(f"timestamp={recent_epoch}" in g for g in globs)


def test_qualifying_partition_globs_empty_when_dataset_empty(hive_incremental_repo):
    assert hive_incremental_repo._qualifying_partition_globs(0) == []


def test_load_incremental_prunes_old_partitions_at_file_level(hive_incremental_repo):
    """Regression guard for the exact bug this was built to fix: globbing the
    whole dataset and filtering on partition_column in the WHERE clause
    measured no faster than an unfiltered scan on the real (large, deeply
    nested) wave-buoy dataset — resolving the full recursive glob dominates
    before any WHERE filter gets a chance to skip anything. Confirms that
    the glob patterns load_incremental() actually uses (built from
    _qualifying_partition_globs, not a WHERE clause) exclude an old
    partition at the file level.

    Three partitions, default lookback_days=5 (cutoff = 5 days ago):
      far_past (1000d ago)  -- older than the cutoff-containing partition:
                                must be excluded
      boundary (10d ago)    -- latest partition at-or-before the cutoff:
                                becomes the threshold, included in full
      recent (1d ago)       -- after the cutoff: included
    """
    con = hive_incremental_repo.session.get_instance()
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    far_past_start = now - pd.Timedelta(days=1000)
    boundary_start = now - pd.Timedelta(days=10)
    recent_start = now - pd.Timedelta(days=1)

    _write_hive_partition(
        con,
        hive_incremental_repo.dataset,
        far_past_start,
        _rows("A", far_past_start, 10.0),
    )
    _write_hive_partition(
        con,
        hive_incremental_repo.dataset,
        boundary_start,
        _rows("B", boundary_start, 20.0),
    )
    _write_hive_partition(
        con, hive_incremental_repo.dataset, recent_start, _rows("C", recent_start, 30.0)
    )

    hive_incremental_repo.load()

    cutoff = hive_incremental_repo._incremental_cutoff(
        hive_incremental_repo.incremental_lookback_days
    )
    cutoff_epoch = int(cutoff.tz_localize("UTC").timestamp())
    globs = hive_incremental_repo._qualifying_partition_globs(cutoff_epoch)

    boundary_epoch = int(boundary_start.tz_localize("UTC").timestamp())
    recent_epoch = int(recent_start.tz_localize("UTC").timestamp())
    far_past_epoch = int(far_past_start.tz_localize("UTC").timestamp())
    assert len(globs) == 2
    assert any(f"timestamp={boundary_epoch}" in g for g in globs)
    assert any(f"timestamp={recent_epoch}" in g for g in globs)
    assert not any(f"timestamp={far_past_epoch}" in g for g in globs)

    plan = con.execute(
        f"""
        EXPLAIN ANALYZE
        SELECT "TIME" FROM read_parquet({globs!r}, hive_partitioning=true, union_by_name=true)
        """
    ).fetchall()
    text = "\n".join(row[1] if len(row) > 1 else str(row) for row in plan)
    # 2 of 3 files: boundary + recent, far_past excluded — because it was
    # never part of the glob at all, not because of a WHERE filter.
    assert "Total Files Read: 2" in text, text


def test_load_uses_full_load_threads_then_restores_default(tmp_path, monkeypatch):
    from data_access_service.config.config import Config
    from data_access_service.models.sites_types import ParquetsGenerationConfig

    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    cfg = ParquetsGenerationConfig(
        duckdb_database=":memory:",
        co_bucket="aodn-cloud-optimised",
        memory_limit="800M",
        threads=4,
        full_load_threads=8,
        duckdb_temp_dir="/tmp",
        region="ap-southeast-2",
        extensions=(),
    )
    monkeypatch.setattr(Config, "get_parquets_config", lambda self: cfg)

    session = ParquetDuckDBClient()
    try:
        repo = _IncrementalRepo(session)
        repo.dataset = str(tmp_path)
        con = session.get_instance()
        _write_file(
            con,
            repo.dataset,
            "d.parquet",
            _rows("A", pd.Timestamp.now(tz="UTC").tz_localize(None), 1.0),
        )

        assert session.threads == 4
        assert session.full_load_threads == 8
        assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 4

        repo.load()

        # Restored to the steady-state value once the full reload finishes.
        assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 4
    finally:
        session.close()
