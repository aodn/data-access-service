"""TaskScheduler's startup-refresh decision: full reload vs. incremental catch-up.

Tests ``_backup_is_fresh`` and ``_initial_refresh_task`` -- whether a
preloaded S3 backup is fresh enough to skip a full reload on startup.
``_refresh_repository`` and the underlying S3 calls are exercised
elsewhere; here it's monkeypatched with a spy to isolate the decision.
"""

from typing import ClassVar

import pandas as pd
import pytest

from data_access_service.core.duckdbclient import DuckDBClient, ParquetDuckDBClient
from data_access_service.core.scheduler import TaskScheduler
from data_access_service.models.sites_types import ParquetsGenerationConfig
from data_access_service.sites.sites_repository import ParquetRepository, quote_ident


class _Repo(ParquetRepository):
    table = "test_repo"
    bucket = "test-bucket"
    backup_bucket = "test-backup"
    dataset = "s3://test-bucket/test_repo.parquet"
    backup_dataset = "s3://test-backup/test_repo.parquet"
    time_column = "TIME"
    site_column = "site_code"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    value_columns: ClassVar[tuple[str, ...]] = ("TEMP",)


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    cfg = ParquetsGenerationConfig(
        duckdb_database=":memory:",
        co_bucket="test-bucket",
        memory_limit="800M",
        threads=4,
        full_load_threads=8,
        duckdb_temp_dir="/tmp",
        region="ap-southeast-2",
        extensions=(),
        incremental_lookback_days=10,
    )
    from data_access_service.config.config import Config

    monkeypatch.setattr(Config, "get_parquets_config", lambda self: cfg)
    s = ParquetDuckDBClient()
    yield s
    s.close()


def _materialize(repo: ParquetRepository, rows: pd.DataFrame) -> None:
    conn = repo.session.get_instance()
    conn.register("_seed_df", rows)
    conn.execute(
        f"CREATE OR REPLACE TABLE {quote_ident(repo.table)} AS SELECT * FROM _seed_df"
    )
    conn.unregister("_seed_df")


def _row(time: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "site_code": ["A"],
            "TIME": [time],
            "LATITUDE": [-30.0],
            "LONGITUDE": [150.0],
            "TEMP": [20.0],
        }
    )


# --- _backup_is_fresh ---------------------------------------------------------


def test_backup_is_fresh_false_when_table_not_loaded(session):
    repo = _Repo(session)
    assert TaskScheduler._backup_is_fresh(repo) is False


def test_backup_is_fresh_false_when_table_empty(session):
    repo = _Repo(session)
    _materialize(repo, _row(pd.Timestamp.now(tz="UTC").tz_localize(None)).iloc[0:0])
    assert TaskScheduler._backup_is_fresh(repo) is False


def test_backup_is_fresh_false_when_latest_row_outside_lookback(session):
    repo = _Repo(session)
    old = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=30)
    _materialize(repo, _row(old))
    assert TaskScheduler._backup_is_fresh(repo) is False


def test_backup_is_fresh_true_when_latest_row_inside_lookback(session):
    repo = _Repo(session)
    recent = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=1)
    _materialize(repo, _row(recent))
    assert TaskScheduler._backup_is_fresh(repo) is True


def test_backup_is_fresh_true_at_exact_cutoff_boundary(session, monkeypatch):
    """load_incremental's cutoff is inclusive (TIME >= cutoff), so freshness
    should agree at the exact boundary. "Now" is frozen so the cutoff
    computed here and _backup_is_fresh's internal one land on the same instant."""
    fixed_now = pd.Timestamp("2026-01-15T00:00:00", tz="UTC")
    monkeypatch.setattr(pd.Timestamp, "now", staticmethod(lambda tz=None: fixed_now))

    repo = _Repo(session)
    cutoff = repo._incremental_cutoff(repo.incremental_lookback_days)
    _materialize(repo, _row(cutoff))
    assert TaskScheduler._backup_is_fresh(repo) is True


# --- _initial_refresh_task -----------------------------------------------------


def test_initial_refresh_task_uses_incremental_for_fresh_repos_full_for_stale(
    session, monkeypatch
):
    fresh_repo = _Repo(session)
    _materialize(
        fresh_repo,
        _row(pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=1)),
    )

    class _StaleRepo(_Repo):
        table = "test_repo_stale"

    stale_repo = _StaleRepo(session)
    _materialize(
        stale_repo,
        _row(pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=30)),
    )

    calls = []
    monkeypatch.setattr(
        TaskScheduler,
        "_refresh_repository",
        lambda self, name, repo, *, incremental=False: calls.append(
            (name, incremental)
        ),
    )

    scheduler = TaskScheduler(
        api=None, repositories={"fresh": fresh_repo, "stale": stale_repo}
    )
    scheduler._initial_refresh_task()

    assert dict(calls) == {"fresh": True, "stale": False}


def test_initial_refresh_task_full_reload_for_never_loaded_repo(session, monkeypatch):
    never_loaded = _Repo(session)  # no _materialize call -> is_loaded() is False

    calls = []
    monkeypatch.setattr(
        TaskScheduler,
        "_refresh_repository",
        lambda self, name, repo, *, incremental=False: calls.append(
            (name, incremental)
        ),
    )

    scheduler = TaskScheduler(api=None, repositories={"never_loaded": never_loaded})
    scheduler._initial_refresh_task()

    assert calls == [("never_loaded", False)]
