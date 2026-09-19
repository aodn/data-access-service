"""Unit tests for TilerDuckDBClient (connection ownership, params, lifecycle).

Always ``:memory:`` — unlike SitesDuckDBClient/EstimationDuckDBClient there is
no S3/httpfs setup to skip, so no config fixture is needed.
"""

import pytest

from data_access_service.core.duckdbclient import TilerDuckDBClient


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
