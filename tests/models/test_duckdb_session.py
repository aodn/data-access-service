"""Unit tests for DuckDBSession (connection ownership, params, lifecycle).

These use ``extensions=()`` so nothing tries to download httpfs over the
network — the behaviours under test don't need S3.
"""

import pytest

from data_access_service.models.duckdb_session import DuckDBSession


def test_execute_returns_relation():
    with DuckDBSession(database=":memory:", extensions=()) as session:
        (value,) = session.execute("SELECT 42").fetchone()
        assert value == 42


def test_execute_binds_params():
    with DuckDBSession(database=":memory:", extensions=()) as session:
        (value,) = session.execute("SELECT ? + ?", [1, 2]).fetchone()
        assert value == 3


def test_tables_persist_across_calls_via_shared_catalog():
    # Each execute() runs on a fresh cursor, but they share the session catalog,
    # so a table created in one call is visible in the next.
    with DuckDBSession(database=":memory:", extensions=()) as session:
        session.execute("CREATE TABLE t AS SELECT 1 AS a UNION ALL SELECT 2")
        (count,) = session.execute("SELECT count(*) FROM t").fetchone()
        assert count == 2


def test_context_manager_closes_connection():
    with DuckDBSession(database=":memory:", extensions=()) as session:
        pass
    # After __exit__ the connection is closed, so further use raises.
    with pytest.raises(Exception):
        session.execute("SELECT 1")


def test_close_is_safe_to_call():
    session = DuckDBSession(database=":memory:", extensions=())
    session.close()
    with pytest.raises(Exception):
        session.execute("SELECT 1")
