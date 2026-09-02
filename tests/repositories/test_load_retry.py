"""Unit tests for ParquetRepository.load()'s retry-on-transient-failure behavior.

The primary dataset is written by an external pipeline, so load() can race an
in-progress rewrite; that surfaces as duckdb.IOException or UnicodeDecodeError.
session.execute() is mocked directly (no real S3/DuckDB read needed), and
time.sleep is patched out so the tests don't actually wait through the retry
backoff.
"""

from unittest.mock import MagicMock

import duckdb
import pytest

from data_access_service.core.duckdbclient import DuckDBClient, SitesDuckDBClient
from data_access_service.sites.sites_repository import ParquetRepository


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda seconds: None)


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    s = SitesDuckDBClient()
    yield s
    s.close()


@pytest.fixture
def repo(session):
    cls = type(
        "_LoadRetryRepo",
        (ParquetRepository,),
        {
            "table": "test_load_retry",
            "bucket": "test-bucket",
            "snapshot_bucket": "test-snapshot",
            "dataset": "s3://test-bucket/test_load_retry.parquet",
            "snapshot_dataset": "s3://test-snapshot/test_load_retry.parquet",
            "time_column": "TIME",
            "site_column": "site_code",
            "latitude_column": "LATITUDE",
            "longitude_column": "LONGITUDE",
            "value_columns": ("TEMP",),
        },
    )
    return cls(session)


def _unicode_decode_error() -> UnicodeDecodeError:
    return UnicodeDecodeError("utf-8", b"\xc0", 0, 1, "invalid start byte")


def test_load_retries_and_succeeds_after_transient_io_exceptions(repo, monkeypatch):
    execute = MagicMock(
        side_effect=[duckdb.IOException("boom"), duckdb.IOException("boom again"), None]
    )
    monkeypatch.setattr(repo.session, "execute", execute)

    result = repo.load()

    assert result is repo
    assert execute.call_count == 3


def test_load_retries_on_unicode_decode_error(repo, monkeypatch):
    execute = MagicMock(side_effect=[_unicode_decode_error(), None])
    monkeypatch.setattr(repo.session, "execute", execute)

    result = repo.load()

    assert result is repo
    assert execute.call_count == 2


def test_load_reraises_after_exhausting_retries(repo, monkeypatch):
    execute = MagicMock(side_effect=duckdb.IOException("persistent failure"))
    monkeypatch.setattr(repo.session, "execute", execute)

    with pytest.raises(duckdb.IOException):
        repo.load()

    assert execute.call_count == 3


def test_load_does_not_retry_other_exceptions(repo, monkeypatch):
    execute = MagicMock(side_effect=ValueError("not a retryable error"))
    monkeypatch.setattr(repo.session, "execute", execute)

    with pytest.raises(ValueError):
        repo.load()

    assert execute.call_count == 1
