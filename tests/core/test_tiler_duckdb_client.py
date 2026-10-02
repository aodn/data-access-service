"""Unit tests for TilerDuckDBClient (connection ownership, params, lifecycle).

Always ``:memory:`` — unlike SitesDuckDBClient/EstimationDuckDBClient there is
no S3/httpfs setup to skip, so no config fixture is needed.
"""

import duckdb
import pytest

from data_access_service.core.duckdbclient import (
    DuckDBClient,
    TilerDuckDBClient,
    is_expired_token,
)


def test_execute_returns_relation():
    with TilerDuckDBClient() as client:
        (value,) = client.execute("SELECT 42").fetchone()
        assert value == 42


def test_execute_binds_params():
    with TilerDuckDBClient() as client:
        (value,) = client.execute("SELECT ? + ?", [1, 2]).fetchone()
        assert value == 3


def test_concurrent_calls_share_one_catalog_via_fresh_cursors():
    # Each execute() runs on a fresh cursor, but they share the session
    # catalog, so a table created in one call is visible in the next.
    with TilerDuckDBClient() as client:
        client.execute("CREATE TABLE t AS SELECT 1 AS a UNION ALL SELECT 2")
        (count,) = client.execute("SELECT count(*) FROM t").fetchone()
        assert count == 2


def test_context_manager_closes_connection():
    with TilerDuckDBClient() as client:
        pass
    # After __exit__ the connection is closed, so further use raises.
    with pytest.raises(Exception):
        client.execute("SELECT 1")


def test_close_is_safe_to_call():
    client = TilerDuckDBClient()
    client.close()
    with pytest.raises(Exception):
        client.execute("SELECT 1")


def test_refresh_recreates_secrets_once_per_interval(monkeypatch):
    created = []
    monkeypatch.setattr(
        DuckDBClient, "create_s3_secret", lambda self, bucket: created.append(bucket)
    )
    with TilerDuckDBClient() as client:
        client.create_s3_secret("bucket-a")
        client.refresh_s3_secrets()
        # A second thread hitting the same expiry doesn't refresh again.
        client.refresh_s3_secrets()
    assert created == ["bucket-a", "bucket-a"]


def test_is_expired_token():
    assert is_expired_token(duckdb.HTTPException("ExpiredToken: token expired"))
    assert is_expired_token(duckdb.HTTPException("TokenRefreshRequired: refresh"))
    assert not is_expired_token(duckdb.HTTPException("HTTP 403 Forbidden"))
    assert not is_expired_token(ValueError("ExpiredToken"))


def test_is_expired_token_mid_stream():
    # An error after the first batch surfaces from pyarrow as OSError.
    with TilerDuckDBClient() as client:
        reader = client.execute(
            "SELECT CASE WHEN i >= 3000000 THEN error('ExpiredToken: expired') "
            "ELSE i END FROM range(5000000) t(i)"
        ).to_arrow_reader(1_000_000)
        with pytest.raises(OSError) as caught:
            for _ in reader:
                pass
    assert is_expired_token(caught.value)
