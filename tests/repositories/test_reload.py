"""Unit tests for the always-on service's snapshot reload path on ParquetRepository:
snapshot_etag and reload_if_changed.

AWSHelper (the S3 HEAD call) is mocked — these test the ETag comparison and
reload orchestration, not real S3. The snapshot itself is a real local parquet
file so `load_snapshot` (called by `reload_if_changed`) is exercised end to end.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from data_access_service.core.duckdbclient import DuckDBClient, ParquetDuckDBClient
from data_access_service.sites.sites_repository import ParquetRepository


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    s = ParquetDuckDBClient()
    yield s
    s.close()


@pytest.fixture
def repo(session, tmp_path):
    cls = type(
        "_ReloadRepo",
        (ParquetRepository,),
        {
            "table": "test_reload",
            "bucket": "test-bucket",
            "snapshot_bucket": "test-snapshot",
            "dataset": "s3://test-bucket/test_reload.parquet",
            "snapshot_dataset": str(tmp_path / "test_reload.parquet"),
            "time_column": "TIME",
            "site_column": "site_code",
            "latitude_column": "LATITUDE",
            "longitude_column": "LONGITUDE",
            "value_columns": ("TEMP",),
        },
    )
    return cls(session)


def _write_snapshot(repo: ParquetRepository) -> None:
    df = pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01"]),
            "site_code": ["A"],
            "LATITUDE": [-30.0],
            "LONGITUDE": [150.0],
            "TEMP": [20.0],
        }
    )
    conn = repo.session.get_instance()
    conn.register("_seed_df", df)
    try:
        conn.execute(
            f"COPY (SELECT * FROM _seed_df) TO '{repo.snapshot_dataset}' (FORMAT PARQUET)"
        )
    finally:
        conn.unregister("_seed_df")


def _mock_aws(monkeypatch, *, etag: str | None = None):
    """Patch the AWSHelper() constructed inside snapshot_etag() with a fake S3 client."""
    aws = MagicMock()
    aws.s3.exceptions.ClientError = ClientError
    if etag is None:
        aws.s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
    else:
        aws.s3.head_object.return_value = {"ETag": etag}
    monkeypatch.setattr(
        "data_access_service.sites.sites_repository.AWSHelper", lambda: aws
    )
    return aws


# --- snapshot_etag -------------------------------------------------------------


def test_snapshot_etag_returns_etag_when_object_exists(repo, monkeypatch):
    _mock_aws(monkeypatch, etag="abc123")
    assert repo.snapshot_etag() == "abc123"


def test_snapshot_etag_none_when_object_missing(repo, monkeypatch):
    _mock_aws(monkeypatch, etag=None)
    assert repo.snapshot_etag() is None


# --- reload_if_changed ----------------------------------------------------------


def test_reload_if_changed_false_when_no_snapshot_yet(repo, monkeypatch):
    _mock_aws(monkeypatch, etag=None)
    assert repo.reload_if_changed() is False
    assert repo.is_loaded() is False


def test_reload_if_changed_true_and_loads_on_first_change(repo, monkeypatch):
    _write_snapshot(repo)
    _mock_aws(monkeypatch, etag="etag-1")
    assert repo.reload_if_changed() is True
    assert repo.is_loaded() is True


def test_reload_if_changed_false_when_etag_unchanged(repo, monkeypatch):
    _write_snapshot(repo)
    _mock_aws(monkeypatch, etag="etag-1")
    assert repo.reload_if_changed() is True

    _mock_aws(monkeypatch, etag="etag-1")
    assert repo.reload_if_changed() is False


def test_reload_if_changed_true_when_etag_changes_again(repo, monkeypatch):
    _write_snapshot(repo)
    _mock_aws(monkeypatch, etag="etag-1")
    assert repo.reload_if_changed() is True

    _mock_aws(monkeypatch, etag="etag-2")
    assert repo.reload_if_changed() is True
