"""Tests for PmTileDuckDBClient teardown safety.

Covers the production failure mode where forked PMTiles workers died with
wait status 139 (SIGSEGV) during pre-tippecanoe DuckDB release after a long
httpfs/h3 staging job. Mock tests lock the close policy; real-DuckDB and
fork tests verify the native teardown path actually survives.
"""

import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from data_access_service.core.duckdbclient import PmTileDuckDBClient


@pytest.fixture(autouse=True)
def _reset_pmtiles_duckdb_globals():
    """Isolate process-global connection state between tests."""
    PmTileDuckDBClient._global_db_connection = None
    PmTileDuckDBClient._temp_dir_object = None
    yield
    # Always tear down leftovers so later tests never inherit a live connection.
    try:
        PmTileDuckDBClient.shutdown()
    except Exception:
        pass
    PmTileDuckDBClient._global_db_connection = None
    PmTileDuckDBClient._temp_dir_object = None


def test_shutdown_closes_connection_without_module_level_duckdb_close():
    """Module-level duckdb.close() after connection.close() can SIGSEGV.

    Regression: commit 38d455f added duckdb.close() "to free memory"; production
    forked workers then died with status 139 during pre-tippecanoe release.
    """
    conn = MagicMock()
    temp_dir = MagicMock()
    PmTileDuckDBClient._global_db_connection = conn
    PmTileDuckDBClient._temp_dir_object = temp_dir

    with patch.object(duckdb, "close", MagicMock()) as module_close:
        PmTileDuckDBClient.shutdown()

    # Soft shrink then native connection close — never module-level close.
    assert conn.execute.call_count >= 1
    conn.close.assert_called_once_with()
    module_close.assert_not_called()
    temp_dir.cleanup.assert_called_once_with()
    assert PmTileDuckDBClient._global_db_connection is None
    assert PmTileDuckDBClient._temp_dir_object is None


def test_shutdown_is_idempotent():
    PmTileDuckDBClient._global_db_connection = None
    PmTileDuckDBClient._temp_dir_object = None
    PmTileDuckDBClient.shutdown()
    PmTileDuckDBClient.shutdown()


def test_shutdown_continues_temp_cleanup_if_connection_close_raises():
    conn = MagicMock()
    conn.close.side_effect = RuntimeError("native boom")
    temp_dir = MagicMock()
    PmTileDuckDBClient._global_db_connection = conn
    PmTileDuckDBClient._temp_dir_object = temp_dir

    PmTileDuckDBClient.shutdown()

    temp_dir.cleanup.assert_called_once_with()
    assert PmTileDuckDBClient._global_db_connection is None


def test_close_nulls_cursor_even_if_cursor_close_raises():
    client = PmTileDuckDBClient.__new__(PmTileDuckDBClient)
    client._lock = threading.Lock()
    client._logger = MagicMock()
    bad_cursor = MagicMock()
    bad_cursor.close.side_effect = RuntimeError("cursor already closed")
    client._duckdb_client = bad_cursor
    client._con = bad_cursor

    client.close()

    assert client._duckdb_client is None
    assert client._con is None
    client._logger.exception.assert_called()


def _run_h3_staging_like_workload(client: PmTileDuckDBClient, work_dir: Path) -> None:
    """Exercise the same native surface as PMTiles staging (h3 + parquet COPY)."""
    source = work_dir / "source.parquet"
    staged = work_dir / "staged.parquet"
    # Build a small parquet via DuckDB so we exercise read_parquet + h3 + COPY
    # the same way HexbinProcessor.build_staging_parquet does (without S3).
    client.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (-42.88, 147.33, TIMESTAMP '2020-01-01'),
                (-42.89, 147.34, TIMESTAMP '2020-01-02'),
                (-33.86, 151.21, TIMESTAMP '2020-02-01'),
                (-33.87, 151.22, TIMESTAMP '2020-02-02')
            ) AS t(LATITUDE, LONGITUDE, TIME)
        ) TO '{source}' (FORMAT PARQUET)
        """
    )
    client.execute(
        f"""
        COPY (
            SELECT
                printf('%x', h3_latlng_to_cell(
                    CAST(LATITUDE AS DOUBLE),
                    CAST(LONGITUDE AS DOUBLE),
                    8
                )) AS h_high,
                strftime(TIME, '%Y%m%d')::INTEGER AS d,
                COUNT(*)::UBIGINT AS c
            FROM read_parquet('{source}')
            WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
            GROUP BY h_high, d
            HAVING h_high IS NOT NULL
        ) TO '{staged}' (FORMAT PARQUET)
        """
    )
    # Match geojsonseq path: temp table + ordered scan of staged aggregates.
    client.execute(
        f"""
        CREATE TEMP TABLE period_counts AS
        SELECT h_high AS h, d, c FROM read_parquet('{staged}')
        """
    )
    n = client.execute("SELECT COUNT(*) FROM period_counts").fetchone()[0]
    assert n >= 1
    client.execute("SELECT h, d, c FROM period_counts ORDER BY h, d").fetchall()


def test_real_client_close_and_shutdown_after_h3_workload_does_not_crash():
    """Native close path after h3/httpfs-style work must complete cleanly.

    Production died here (SIGSEGV / status 139) when shutdown also called
    module-level duckdb.close() after connection.close().
    """
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        client = PmTileDuckDBClient()
        assert PmTileDuckDBClient._global_db_connection is not None

        with patch.object(duckdb, "close", wraps=duckdb.close) as module_close:
            _run_h3_staging_like_workload(client, work)
            # Same order as AbstractProcessor._release_duckdb
            client.close()
            PmTileDuckDBClient.shutdown()
            module_close.assert_not_called()

        assert PmTileDuckDBClient._global_db_connection is None
        assert PmTileDuckDBClient._temp_dir_object is None

        # A later dataset must be able to rebuild a fresh connection.
        client2 = PmTileDuckDBClient()
        assert client2.execute("SELECT 1").fetchone() == (1,)
        client2.close()
        PmTileDuckDBClient.shutdown()


def test_release_duckdb_order_survives_in_forked_child():
    """Forked worker must exit 0 after close+shutdown, not SIGSEGV (status 139).

    This is the exact failure mode from PMTiles batch logs:
    parent saw WIFSIGNALED / termsig=11 after pre-tippecanoe DuckDB release.
    """
    pid = os.fork()
    if pid == 0:
        # Child: never return into pytest parent.
        code = 1
        try:
            PmTileDuckDBClient._global_db_connection = None
            PmTileDuckDBClient._temp_dir_object = None
            with tempfile.TemporaryDirectory() as tmp:
                client = PmTileDuckDBClient()
                _run_h3_staging_like_workload(client, Path(tmp))
                client.close()
                PmTileDuckDBClient.shutdown()
            code = 0
        except BaseException:
            code = 1
        os._exit(code)

    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status), (
        f"child did not exit normally: status={status} "
        f"termsig={os.WTERMSIG(status) if os.WIFSIGNALED(status) else None} "
        f"(139/SIGSEGV indicates DuckDB teardown crash)"
    )
    assert (
        os.WEXITSTATUS(status) == 0
    ), f"child exited with code {os.WEXITSTATUS(status)}, expected 0"


def _make_local_pmtiles_client():
    """Build a client without requiring valid AWS credentials."""
    with patch.object(PmTileDuckDBClient, "create_s3_secret", return_value=None):
        return PmTileDuckDBClient()


def test_detect_time_type_timestamp_column():
    """Native TIMESTAMP columns stay as timestamp (no epoch conversion)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "ts.parquet"
        client = _make_local_pmtiles_client()
        client.execute(
            f"""
            COPY (
                SELECT TIMESTAMP '2025-12-01 00:00:00' AS detection_timestamp
            ) TO '{path}' (FORMAT PARQUET)
            """
        )
        assert client.detect_time_type(str(path), "detection_timestamp") == "timestamp"


def test_detect_time_type_epoch_seconds_as_bigint():
    """BIGINT epoch *seconds* (~1e9) must not be classified as milliseconds.

    Regression: detect_time_type used to map any BIGINT -> epoch_ms, which
    turned hive partition keys like timestamp=1764547200 into 1970-01-21.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "epoch_s.parquet"
        client = _make_local_pmtiles_client()
        # 2025-12-01 / 2026-05-01 as Unix seconds (same scale as animal_metadata
        # satellite-relay hive partitions).
        client.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (1764547200::BIGINT),
                    (1777593600::BIGINT)
                ) AS t(timestamp)
            ) TO '{path}' (FORMAT PARQUET)
            """
        )
        assert client.detect_time_type(str(path), "timestamp") == "epoch_s"

        client.execute("SET TimeZone = 'UTC'")
        d_sql = PmTileDuckDBClient.build_date_key_expression("timestamp", "epoch_s")
        keys = {
            r[0]
            for r in client.execute(
                f"SELECT DISTINCT {d_sql} FROM read_parquet('{path}')"
            ).fetchall()
        }
        assert keys == {20251201, 20260501}


def test_detect_time_type_epoch_milliseconds_as_bigint():
    """BIGINT epoch *milliseconds* (~1e12) still classify as epoch_ms."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "epoch_ms.parquet"
        client = _make_local_pmtiles_client()
        # 2025-12-01 00:00:00 UTC in milliseconds.
        client.execute(
            f"""
            COPY (
                SELECT (1764547200::BIGINT * 1000) AS timestamp
            ) TO '{path}' (FORMAT PARQUET)
            """
        )
        assert client.detect_time_type(str(path), "timestamp") == "epoch_ms"

        client.execute("SET TimeZone = 'UTC'")
        d_sql = PmTileDuckDBClient.build_date_key_expression("timestamp", "epoch_ms")
        key = client.execute(f"SELECT {d_sql} FROM read_parquet('{path}')").fetchone()[
            0
        ]
        assert key == 20251201


def test_detect_time_type_hive_partition_epoch_seconds():
    """Hive partition keys are BIGINT; values in seconds must yield epoch_s.

    Mirrors animal_metadata_satellite_relay_tagging_realtime_qc.parquet layout:
    timestamp=<unix_s>/.../*.parquet
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "dataset.parquet"
        part_dir = root / "timestamp=1764547200"
        part_dir.mkdir(parents=True)
        part_file = part_dir / "part-0.parquet"

        client = _make_local_pmtiles_client()
        client.execute(
            f"""
            COPY (
                SELECT -49.35::DOUBLE AS lat, 70.22::DOUBLE AS lon
            ) TO '{part_file}' (FORMAT PARQUET)
            """
        )

        glob_path = str(root / "**" / "*.parquet")
        assert client.detect_time_type(glob_path, "timestamp") == "epoch_s"

        client.execute("SET TimeZone = 'UTC'")
        d_sql = PmTileDuckDBClient.build_date_key_expression("timestamp", "epoch_s")
        key = client.execute(
            f"""
            SELECT {d_sql}
            FROM read_parquet('{glob_path}', hive_partitioning=true)
            """
        ).fetchone()[0]
        assert key == 20251201


def test_build_date_key_epoch_s_vs_epoch_ms_expression():
    s = PmTileDuckDBClient.build_date_key_expression("timestamp", "epoch_s")
    ms = PmTileDuckDBClient.build_date_key_expression("timestamp", "epoch_ms")
    assert "/ 1000.0" not in s
    assert "/ 1000.0" in ms
    assert "%Y%m%d" in s and "%Y%m%d" in ms
