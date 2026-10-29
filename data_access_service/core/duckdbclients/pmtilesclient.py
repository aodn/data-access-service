from data_access_service.core.duckdbclients.basedbclient import (
    DuckDBClient,
    _PROGRESS_LOG_INTERVAL_SECONDS,
)
from threading import Lock
from datetime import timedelta
from data_access_service.models.duckdb_types import DuckDBTuningConfig
from typing import Any, Optional
from data_access_service import Config
from tempfile import TemporaryDirectory
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from data_access_service.utils.retry_utils import log_retry_attempt
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import os
import duckdb
import threading
import time


class PmTileDuckDBClient(DuckDBClient):
    # Process-global singletons
    _global_db_connection = None
    _temp_dir_object = None
    # Tuning the live global connection was opened with, so a client asking for
    # different settings rebuilds instead of silently getting the old ones.
    _global_tuning = None
    _lock = Lock()
    # Temp-dir objects a forked child inherited from its parent. Kept referenced
    # so their finalizer cannot run in the child and delete a directory the
    # parent is still using.
    _inherited_temp_dirs: list = []

    READ_MIN_WAIT = timedelta(minutes=2)
    READ_MAX_WAIT = timedelta(minutes=5)
    READ_MAX_ATTEMPTS = 3

    def __init__(self, tuning: Optional[DuckDBTuningConfig] = None):
        """Open a session with ``tuning``, defaulting to the pmtiles job's settings.

        Batch jobs pass their own object so one job's memory limit cannot move
        another's; see :class:`DuckDBTuningConfig`.
        """
        super().__init__()
        self._config: DuckDBTuningConfig = (
            tuning or Config.get_config().get_pmtiles_duckdb_tuning()
        )
        self._duckdb_client = None
        self._con = self.get_instance()
        self._lock = Lock()
        # dataset root -> _common_metadata table expression (None when it has none)
        self._common_metadata_sql_cache: dict[str, Optional[str]] = {}

    def get_instance(self):
        """Initializes the single global DB instance if it does not exist."""
        # The connection is process-global but the settings are per job, so a
        # job that follows another one in the same process (estimation after
        # pmtiles) must not inherit the previous job's limits.
        if (
            PmTileDuckDBClient._global_db_connection is not None
            and PmTileDuckDBClient._global_tuning != self._config
        ):
            PmTileDuckDBClient._logger.info(
                "DuckDB connection was opened with different tuning (%s); "
                "rebuilding it for %s",
                PmTileDuckDBClient._global_tuning,
                self._config,
            )
            PmTileDuckDBClient.shutdown()

        if PmTileDuckDBClient._global_db_connection is None:
            with PmTileDuckDBClient._lock:
                if PmTileDuckDBClient._global_db_connection is None:
                    # Keep temporary directory alive at the class level
                    PmTileDuckDBClient._temp_dir_object = TemporaryDirectory(
                        prefix=self._config.duckdb_temp_dir
                    )

                    # Get the absolute string path of the generated temp folder
                    temp_path = PmTileDuckDBClient._temp_dir_object.name

                    db_config = {
                        "temp_directory": temp_path,
                        "memory_limit": self._config.memory_limit,
                        "threads": str(int(self._config.threads)),
                        "preserve_insertion_order": "false",
                        "unsafe_disable_etag_checks": "true",
                        "s3_region": "ap-southeast-2",
                        "s3_access_key_id": "",
                        "s3_secret_access_key": "",
                        "s3_session_token": "",
                        "s3_use_ssl": "true",
                        "s3_url_style": "path",
                        "enable_http_metadata_cache": "true",
                        "http_keep_alive": "true",
                        "enable_external_file_cache": (
                            "true"
                            if self._config.enable_external_file_cache
                            else "false"
                        ),
                    }

                    # Establish the primary process-global connection
                    if self._config.duckdb_database == ":memory:":
                        db = self._connect(
                            self._config.duckdb_database, config=db_config
                        )
                    else:
                        target_database = os.path.join(
                            temp_path, self._config.duckdb_database
                        )
                        db = self._connect(target_database, config=db_config)

                    db.execute("INSTALL h3 FROM community; LOAD h3;")

                    PmTileDuckDBClient._global_db_connection = db
                    PmTileDuckDBClient._global_tuning = self._config

        # CRITICAL: Return a thread-safe, independent cursor from the global connection.
        # Progress-bar settings are LOCAL scope, so they must be set on this cursor
        # (not the parent connection). Terminal print is off: DuckDB writes \r bars
        # to stdout, which never reach uvicorn/app logs. Progress is logged from
        # execute() via the Python logger instead.
        if self._duckdb_client is None:
            with self._lock:
                if self._duckdb_client is None:
                    cursor = PmTileDuckDBClient._global_db_connection.cursor()
                    show = bool(self._config.show_progress)
                    cursor.execute(f"SET enable_progress_bar = {show};")
                    cursor.execute(f"SET enable_progress_bar_print = {show};")
                    self._duckdb_client = cursor
                    # Set before the secret: the cache keys on ``_con``, and
                    # ``__init__`` only assigns it after ``get_instance`` returns.
                    self._con = cursor
                    # Avoid NewRelic capture the log which is too huge and unless
                    self.create_s3_secret(self._config.co_bucket)
        return self._duckdb_client

    def close(self):
        """Close this client's cursor only (not the process-global connection)."""
        with self._lock:
            cursor = self._duckdb_client
            self._duckdb_client = None
            self._con = None
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    PmTileDuckDBClient._logger.exception(
                        "Failed to close PmTileDuckDBClient cursor; continuing shutdown"
                    )

    @classmethod
    def _shrink_connection_memory(cls, connection) -> None:
        """Best-effort shrink of DuckDB buffer pool before native teardown.

        Lowering ``memory_limit`` / threads can release cached pages without the
        more aggressive (and historically crash-prone) module-level ``duckdb.close()``.
        Failures are ignored: the connection is about to be closed anyway.
        """
        for sql in (
            "SET threads TO 1",
            "SET memory_limit = '64MB'",
        ):
            try:
                connection.execute(sql)
            except Exception:
                PmTileDuckDBClient._logger.debug(
                    "Pre-close DuckDB setting failed: %s", sql, exc_info=True
                )

    @classmethod
    def reset_after_fork(cls) -> None:
        """Forget the inherited connection so a forked child opens its own.

        Call this first thing in the child. The parent's handle is never closed
        here: the copy shares native state with the parent's (and none of its
        background threads survived the fork), so touching it can break the
        parent's session. The child just leaves it behind.
        """
        if cls._temp_dir_object is not None:
            # Never drop the last reference: TemporaryDirectory deletes the
            # directory when it is garbage collected, and that directory
            # belongs to the parent.
            cls._inherited_temp_dirs.append(cls._temp_dir_object)
        cls._global_db_connection = None
        cls._global_tuning = None
        cls._temp_dir_object = None
        # A lock copied mid-fork can be held by a thread that does not exist
        # in this process, which would deadlock the first get_instance.
        cls._lock = Lock()

    @classmethod
    def discard_temp_directory(cls) -> None:
        """Delete the temp directory without closing the connection.

        For a forked child that ends in ``os._exit``: no finalizer runs there,
        so the directory would be left behind. Unlinking files DuckDB still has
        open is safe on Linux; the space returns when the process exits.
        """
        temp_dir = cls._temp_dir_object
        cls._temp_dir_object = None
        if temp_dir is None:
            return
        try:
            temp_dir.cleanup()
        except Exception:
            PmTileDuckDBClient._logger.debug(
                "Discarding DuckDB temp directory failed", exc_info=True
            )

    @classmethod
    def shutdown(cls) -> None:
        """Close the process-global connection and remove its temp directory.

        Idempotent when already shut down. Callers use this:

        * After geojsonseq/metadata and **before tippecanoe**, so the buffer
          pool (up to ``memory_limit``) is free for the tippecanoe subprocess
          on memory-constrained Batch hosts.
        * Between datasets in a multi-dataset batch run, so RSS does not
          ratchet up via the HTTP metadata cache and buffer pool.

        Any cursors from :meth:`get_instance` become invalid. The next
        ``get_instance`` call (e.g. a new client for the next dataset) lazily
        rebuilds a fresh connection.

        Note: do **not** call module-level ``duckdb.close()`` after
        ``connection.close()`` — that double-teardown path has been observed to
        SIGSEGV the forked PMTiles worker after a long httpfs/h3 staging job.
        """
        with cls._lock:
            connection = cls._global_db_connection
            temp_dir = cls._temp_dir_object
            # Drop the references first so a failure below can never leave
            # later datasets reusing a half-closed connection.
            cls._global_db_connection = None
            cls._global_tuning = None
            cls._temp_dir_object = None

        # Close outside the class lock so a slow/native close cannot stall
        # another thread that only needs to observe "already shut down".
        try:
            if connection is not None:
                PmTileDuckDBClient._logger.info(
                    "DuckDB shutdown: shrinking buffer pool"
                )
                cls._shrink_connection_memory(connection)
                PmTileDuckDBClient._logger.info("DuckDB shutdown: closing connection")
                try:
                    connection.close()
                except Exception:
                    PmTileDuckDBClient._logger.exception(
                        "DuckDB connection.close() failed; continuing temp cleanup"
                    )
                PmTileDuckDBClient._logger.info("DuckDB shutdown: connection closed")
        finally:
            if temp_dir is not None:
                try:
                    PmTileDuckDBClient._logger.info(
                        "DuckDB shutdown: cleaning temp directory"
                    )
                    temp_dir.cleanup()
                except Exception:
                    PmTileDuckDBClient._logger.exception(
                        "DuckDB temp directory cleanup failed"
                    )

    # Bug in tenacity, the type check always fail but function ok
    # noinspection PyCallingNonCallable
    @retry(
        stop=stop_after_attempt(READ_MAX_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=READ_MIN_WAIT, max=READ_MAX_WAIT),
        retry=retry_if_exception_type(duckdb.IOException),
        before_sleep=log_retry_attempt("DuckDB S3 read", DuckDBClient._logger),
        reraise=True,
    )
    def execute(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        def run():
            if params is None:
                return self._duckdb_client.execute(sql)
            return self._duckdb_client.execute(sql, params)

        if not self._config.show_progress:
            return self._call_refreshing_s3(run)
        with self._progress_logger(sql):
            return self._call_refreshing_s3(run)

    @contextmanager
    def _progress_logger(self, sql: str) -> Iterator[None]:
        """Periodically log DuckDB query progress while ``sql`` runs.

        Uses :meth:`duckdb.DuckDBPyConnection.query_progress` on a background
        thread so long ``COPY`` / aggregation queries emit heartbeat lines into
        the app logger (visible under uvicorn) instead of silent multi-hour waits.
        """
        stop = threading.Event()
        started = time.monotonic()
        sql_preview = DuckDBClient._sql_preview(sql)
        connection = self._duckdb_client

        def _poll() -> None:
            while not stop.wait(_PROGRESS_LOG_INTERVAL_SECONDS):
                elapsed = time.monotonic() - started
                try:
                    pct = connection.query_progress()
                except Exception:
                    pct = -1.0
                if pct is not None and pct >= 0:
                    PmTileDuckDBClient._logger.info(
                        "DuckDB query progress: %.1f%% (%.0fs elapsed) — %s",
                        pct,
                        elapsed,
                        sql_preview,
                    )
                else:
                    PmTileDuckDBClient._logger.info(
                        "DuckDB query still running (%.0fs elapsed) — %s",
                        elapsed,
                        sql_preview,
                    )

        poller = threading.Thread(target=_poll, name="duckdb-progress", daemon=True)
        poller.start()
        PmTileDuckDBClient._logger.info("DuckDB query started — %s", sql_preview)
        try:
            yield
        finally:
            stop.set()
            poller.join(timeout=1.0)
            PmTileDuckDBClient._logger.info(
                "DuckDB query finished (%.1fs) — %s",
                time.monotonic() - started,
                sql_preview,
            )

    # Sibling file a cloud-optimised dataset publishes next to its partitions:
    # the whole dataset schema in one object.
    _COMMON_METADATA = "_common_metadata"

    # Rows read to tell epoch seconds from milliseconds.
    _EPOCH_SAMPLE_ROWS = 1000

    # Epoch values above this absolute magnitude are treated as milliseconds.
    # Modern epoch-seconds are ~1e9; epoch-milliseconds are ~1e12. 1e11 is well
    # above any realistic second-based timestamp (year ~5138) and below any
    # modern millisecond timestamp (ms since ~1973).
    _EPOCH_MS_ABS_THRESHOLD = 1e11

    _TIMESTAMP_SQL_TYPES = frozenset(
        {
            "TIMESTAMP",
            "TIMESTAMP_NS",
            "TIMESTAMP_MS",
            "TIMESTAMP_S",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMPTZ",
            "DATE",
            "INTERVAL",
        }
    )
    _NUMERIC_SQL_TYPES = frozenset(
        {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "FLOAT",
            "DOUBLE",
            "REAL",
            "DECIMAL",
            "NUMERIC",
        }
    )

    def detect_time_type(
        self,
        input_path: str,
        time_col: str,
    ) -> str:
        """Infer how ``time_col`` stores time in the parquet dataset.

        Returns one of ``"timestamp"``, ``"epoch_ms"``, ``"epoch_s"``.

        Integer / numeric columns are classified by magnitude of sampled values
        rather than DuckDB's SQL type alone: Hive partition keys are always
        promoted to ``BIGINT``, so type-based heuristics mis-label epoch
        *seconds* (e.g. ``timestamp=1764547200``) as milliseconds and produce
        1970-01-21 date keys.
        """
        col_quoted = PmTileDuckDBClient.quote_identifier(time_col)
        source_sql = self.parquet_source_sql(input_path)

        # The type of one column is all this needs, so ask the dataset's
        # _common_metadata first: one request, instead of one per file.
        col_type = None
        metadata_sql = self._common_metadata_sql(input_path)
        if metadata_sql is not None:
            col_type = self._describe_column_type(metadata_sql, time_col, col_quoted)
            if col_type is None:
                PmTileDuckDBClient._logger.info(
                    "detect_time_type: %s is not in %s (Hive key only?); reading "
                    "the schema from the dataset itself",
                    time_col,
                    self._COMMON_METADATA,
                )

        if col_type is None:
            col_type = self._describe_column_type(source_sql, time_col, col_quoted)

        if col_type is None:
            PmTileDuckDBClient._logger.warning(
                "detect_time_type: could not read the type of %s in %s; "
                "defaulting to timestamp",
                time_col,
                input_path,
            )
            return "timestamp"

        base_type = col_type.split("(", 1)[0].strip()

        if base_type in self._TIMESTAMP_SQL_TYPES or base_type.startswith("TIMESTAMP"):
            PmTileDuckDBClient._logger.info(
                "detect_time_type: column=%s sql_type=%s -> timestamp",
                time_col,
                col_type,
            )
            return "timestamp"

        if base_type in self._NUMERIC_SQL_TYPES:
            time_type = self._detect_epoch_unit(source_sql, col_quoted)
            PmTileDuckDBClient._logger.info(
                "detect_time_type: column=%s sql_type=%s -> %s",
                time_col,
                col_type,
                time_type,
            )
            return time_type

        PmTileDuckDBClient._logger.info(
            "detect_time_type: column=%s sql_type=%s (unhandled) -> timestamp",
            time_col,
            col_type,
        )
        return "timestamp"

    def parquet_source_sql(self, input_path: str) -> str:
        """SQL table expression that exposes both file columns and Hive keys."""
        # Prefer the full dataset glob with hive_partitioning so partition
        # columns (e.g. timestamp=1764547200/) are part of the schema. A single
        # file path without hive_partitioning would hide those keys.
        if input_path.endswith("_metadata") or input_path.endswith("_common_metadata"):
            return f"read_parquet('{input_path}')"

        union_by_name = self.needs_union_by_name(input_path)
        return (
            f"read_parquet('{input_path}', hive_partitioning=true, "
            f"union_by_name={str(union_by_name).lower()})"
        )

    def needs_union_by_name(self, input_path: str) -> bool:
        """True when the dataset has no ``_common_metadata`` to bind from.

        union_by_name has to open EVERY file's footer before the query can be
        bound, and keeps that metadata resident: argo (301,875 files, ~109 KB of
        footer each) needs ~31 GB for the binding alone. A dataset that
        publishes ``_common_metadata`` has one canonical schema, so DuckDB can
        bind from the first file and read the rest while it scans.
        """
        return self._common_metadata_sql(input_path) is None

    @staticmethod
    def parquet_file_list_sql(paths: Sequence[str], union_by_name: bool) -> str:
        """The same table expression as :meth:`parquet_source_sql`, for a list of files.

        An explicit path list still carries the Hive keys, so the date-key
        expression built for the whole dataset works unchanged on a subset.
        Takes ``union_by_name`` as an argument rather than probing for
        ``_common_metadata``: the caller already knows, and the probe needs a
        connection the caller may not want to open.
        """
        quoted = ", ".join("'" + path.replace("'", "''") + "'" for path in paths)
        return (
            f"read_parquet([{quoted}], hive_partitioning=true, "
            f"union_by_name={str(union_by_name).lower()})"
        )

    def _dataset_root(self, input_path: str) -> Optional[str]:
        """Root of a dataset glob (``s3://b/x.parquet/**/*.parquet``), else None."""
        head, glob_marker, _ = input_path.partition("/**")
        return head if glob_marker else None

    def _common_metadata_sql(self, input_path: str) -> Optional[str]:
        """Table expression for the dataset's ``_common_metadata``, or None.

        Cached per dataset: the probe is one request, but the answer is needed
        every time SQL is built for that dataset.
        """
        root = self._dataset_root(input_path)
        if root is None:
            return None

        if root not in self._common_metadata_sql_cache:
            sql = f"read_parquet('{root}/{self._COMMON_METADATA}')"
            try:
                self._con.execute(f"DESCRIBE SELECT * FROM {sql} LIMIT 0").fetchall()
            except Exception:
                PmTileDuckDBClient._logger.info(
                    "No readable %s under %s; queries keep union_by_name=true",
                    self._COMMON_METADATA,
                    root,
                )
                sql = None
            self._common_metadata_sql_cache[root] = sql
        return self._common_metadata_sql_cache[root]

    def _describe_column_type(
        self, source_sql: str, time_col: str, col_quoted: str
    ) -> Optional[str]:
        """SQL type of ``time_col`` in ``source_sql``, or None when unreadable."""
        try:
            rows = self._con.execute(
                f"DESCRIBE SELECT {col_quoted} FROM {source_sql} LIMIT 0"
            ).fetchall()
        except Exception:
            PmTileDuckDBClient._logger.debug(
                "DESCRIBE %s in %s failed", time_col, source_sql, exc_info=True
            )
            return None
        for row in rows:
            if row[0].lower() == time_col.lower():
                return row[1].upper()
        return None

    def _detect_epoch_unit(self, source_sql: str, col_quoted: str) -> str:
        """Classify numeric epoch values as seconds or milliseconds by magnitude."""
        # A sample, not the whole dataset: seconds and milliseconds are 1000x
        # apart, so any real row settles it, while MAX() over the source would
        # scan every file.
        try:
            max_abs = self._con.execute(
                f"""
                SELECT MAX(ABS(CAST({col_quoted} AS DOUBLE)))
                FROM (
                    SELECT {col_quoted}
                    FROM {source_sql}
                    WHERE {col_quoted} IS NOT NULL
                    LIMIT {self._EPOCH_SAMPLE_ROWS}
                )
                """
            ).fetchone()[0]
        except Exception:
            PmTileDuckDBClient._logger.warning(
                "detect_time_type: failed to sample numeric values; defaulting to epoch_s",
                exc_info=True,
            )
            return "epoch_s"

        if max_abs is None:
            return "epoch_s"

        if float(max_abs) >= self._EPOCH_MS_ABS_THRESHOLD:
            return "epoch_ms"
        return "epoch_s"

    @staticmethod
    def quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def _timestamp_sql(time_col: str, time_type: str) -> str:
        col = PmTileDuckDBClient.quote_identifier(time_col)

        if time_type == "timestamp":
            return f"CAST({col} AS TIMESTAMP)"
        if time_type == "epoch_ms":
            return f"to_timestamp(CAST({col} AS DOUBLE) / 1000.0)"
        if time_type == "epoch_s":
            return f"to_timestamp(CAST({col} AS DOUBLE))"
        raise ValueError(
            f"Unsupported time_type={time_type!r}. Expected one of: 'timestamp', 'epoch_ms', 'epoch_s'."
        )

    @staticmethod
    def build_ym_expression(time_col: str, time_type: str) -> str:
        ts = PmTileDuckDBClient._timestamp_sql(time_col, time_type)
        return f"CAST(strftime({ts}, '%Y%m') AS INTEGER)"

    @staticmethod
    def build_date_key_expression(time_col: str, time_type: str) -> str:
        ts = PmTileDuckDBClient._timestamp_sql(time_col, time_type)
        return f"CAST(strftime({ts}, '%Y%m%d') AS INTEGER)"

    @staticmethod
    def build_year_key_expression(time_col: str, time_type: str) -> str:
        ts = PmTileDuckDBClient._timestamp_sql(time_col, time_type)
        return f"CAST(strftime({ts}, '%Y') AS INTEGER)"
