from __future__ import annotations

import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Sequence
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Any

import boto3
import duckdb

from data_access_service.models.pmtiles_types import (
    ParquetsGenerationConfig,
    PmtilesGenerationConfig,
)
from data_access_service import Config
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


class DuckDBClient(ABC):
    """Common interface over a DuckDB connection.

    Concrete clients own (or share) a DuckDB connection and expose a uniform
    contract: :meth:`get_instance` to obtain/lazily initialize the underlying
    handle, :meth:`execute` to run SQL, and :meth:`close` to release resources.
    Subclasses decide the connection's lifetime and concurrency model — e.g.
    :class:`PmTileDuckDBClient` shares one process-global connection, while
    :class:`ParquetDuckDBClient` owns one connection and runs each call on its
    own cursor.
    """

    @abstractmethod
    def get_instance(self):
        """Return the underlying DuckDB handle, initializing it if needed."""

    @abstractmethod
    def execute(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Run ``sql`` (optionally with bound ``params``) and return the relation."""

    @abstractmethod
    def close(self) -> None:
        """Release the connection or cursor held by this client."""


class PmTileDuckDBClient(DuckDBClient):
    # Process-global singletons
    _global_db_connection = None
    _temp_dir_object = None
    _lock = Lock()

    MAX_READ_ATTEMPTS = 3
    MIN_WAIT_SECONDS = 120  # 2 minutes
    MAX_WAIT_SECONDS = 300  # 5 minutes

    def log_retry_attempt(self, retry_state):
        # Extract metadata from tenacity's internal state
        attempt_num = retry_state.attempt_number
        exception_thrown = retry_state.outcome.exception()
        next_wait_seconds = retry_state.next_action.sleep
        next_wait_minutes = round(next_wait_seconds / 60, 1)

        self.logger.warning(
            f"[Retry Alert] DuckDB S3 read failed on attempt #{attempt_num}.\n"
            f"Error details: {exception_thrown}\n"
            f"Waiting {next_wait_minutes} minute(s) before attempt #{attempt_num + 1}..."
        )

    def __init__(self):
        self._config: PmtilesGenerationConfig = Config.get_config().get_pmtiles_config()
        self._duckdb_client = None
        self._con = self.get_instance()
        self._lock = Lock()

    def get_instance(self):
        """Initializes the single global DB instance if it does not exist."""
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
                    }

                    # Establish the primary process-global connection
                    if self._config.duckdb_database == ":memory:":
                        db = duckdb.connect(
                            self._config.duckdb_database, config=db_config
                        )
                    else:
                        target_database = os.path.join(
                            temp_path, self._config.duckdb_database
                        )
                        db = duckdb.connect(target_database, config=db_config)

                    db.execute("INSTALL httpfs; LOAD httpfs;")
                    db.execute("INSTALL h3 FROM community; LOAD h3;")
                    db.execute("SET enable_progress_bar = true;")
                    db.execute("SET enable_progress_bar_print = true;")

                    PmTileDuckDBClient._global_db_connection = db

        # CRITICAL: Return a thread-safe, independent cursor from the global connection
        if self._duckdb_client is None:
            with self._lock:
                if self._duckdb_client is None:
                    self._duckdb_client = (
                        PmTileDuckDBClient._global_db_connection.cursor()
                    )
        return self._duckdb_client

    def close(self):
        if self._duckdb_client is not None:
            with self._lock:
                self._duckdb_client.close()
        self._duckdb_client = None

    @classmethod
    def shutdown(cls) -> None:
        """Close the process-global connection and remove its temp directory.

        The batch loop processes datasets sequentially in one long-lived
        process; without this, the global connection's buffer pool (up to
        ``memory_limit``) and HTTP metadata cache accumulate across datasets
        and never return to the OS. Any cursors from :meth:`get_instance`
        become invalid, so only call this once a dataset is fully processed.
        The next ``get_instance`` call lazily rebuilds a fresh connection.
        """
        with cls._lock:
            connection = cls._global_db_connection
            temp_dir = cls._temp_dir_object
            # Drop the references first so a failure below can never leave
            # later datasets reusing a half-closed connection.
            cls._global_db_connection = None
            cls._temp_dir_object = None
            try:
                if connection is not None:
                    connection.close()
            finally:
                if temp_dir is not None:
                    temp_dir.cleanup()

    @retry(
        stop=stop_after_attempt(MAX_READ_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=MIN_WAIT_SECONDS, max=MAX_WAIT_SECONDS),
        retry=retry_if_exception_type(duckdb.IOException),
        before_sleep=lambda retry_state: retry_state.fn.__self__.log_retry_attempt(
            retry_state
        ),
        reraise=True,
    )
    def execute(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        if params is None:
            return self._con.execute(sql)
        return self._con.execute(sql, params)

    def detect_time_type(
        self,
        input_path: str,
        time_col: str,
    ) -> str:
        col_quoted = PmTileDuckDBClient.quote_identifier(time_col)

        sample_path = input_path
        if not (
            input_path.endswith("_metadata") or input_path.endswith("_common_metadata")
        ):
            try:
                files = self._con.execute(
                    f"SELECT * FROM glob('{input_path}') LIMIT 1"
                ).fetchall()
                if files:
                    sample_path = files[0][0]
            except Exception:
                pass

        try:
            rows = self._con.execute(
                f"DESCRIBE SELECT {col_quoted} FROM read_parquet('{sample_path}') LIMIT 0"
            ).fetchall()
        except Exception:
            return "timestamp"

        col_type = None
        for row in rows:
            if row[0].lower() == time_col.lower():
                col_type = row[1].upper()
                break

        if col_type is None:
            raise ValueError(
                f"Column '{time_col}' not found in schema of '{input_path}'."
            )

        if any(t in col_type for t in ("TIMESTAMP", "DATE", "INTERVAL")):
            return "timestamp"
        if any(t in col_type for t in ("BIGINT", "HUGEINT", "UBIGINT")):
            return "epoch_ms"
        return "epoch_s"

    @staticmethod
    def quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def build_date_key_expression(time_col: str, time_type: str) -> str:
        col = PmTileDuckDBClient.quote_identifier(time_col)

        if time_type == "timestamp":
            ts = f"CAST({col} AS TIMESTAMP)"
        elif time_type == "epoch_ms":
            ts = f"to_timestamp(CAST({col} AS DOUBLE) / 1000.0)"
        elif time_type == "epoch_s":
            ts = f"to_timestamp(CAST({col} AS DOUBLE))"
        else:
            raise ValueError(
                f"Unsupported time_type={time_type!r}. Expected one of: 'timestamp', 'epoch_ms', 'epoch_s'."
            )

        return f"CAST(strftime({ts}, '%Y%m%d') AS INTEGER)"


class ParquetDuckDBClient(DuckDBClient):
    """Owns a DuckDB connection and its extension/region configuration.

    A concrete :class:`DuckDBClient`. Like :class:`PmTileDuckDBClient` it builds
    its connection lazily in :meth:`get_instance` (called once from
    ``__init__``) and caches the handle in ``self._con``. It differs in lifetime
    and concurrency: the connection is owned per-instance (not process-global),
    and every :meth:`execute` runs on its own cursor so the threadpool serving
    sync API endpoints can read in parallel.

    Loads the requested extensions and sets the S3 region. The client does not
    decide *which* buckets get credentials — each
    :class:`~data_access_service.sites.duckdb_repository.ParquetRepository`
    calls :meth:`create_s3_secret` for its own buckets on construction (see
    ``ParquetRepository._configure_s3``). The client owns the secret SQL and the
    boto3 plumbing; the repository owns the bucket choice. Usable as a context
    manager.
    """

    def __init__(self) -> None:
        # All settings come from the config (tests override
        # ``Config.get_parquets_config`` to point at an in-memory DB).
        self._config: ParquetsGenerationConfig = (
            Config.get_config().get_parquets_config()
        )
        self._database = self._config.duckdb_database
        self._region = self._config.region
        self._extensions = tuple(self._config.extensions)
        self._duckdb_client = None
        # Track active cursors so they can be interrupted on close.
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        self._lock = Lock()
        self._con = self.get_instance()

    def get_instance(self) -> duckdb.DuckDBPyConnection:
        """Initialize this client's owned connection if it does not exist.

        Mirrors :meth:`PmTileDuckDBClient.get_instance` — lazy, double-checked
        creation under a lock — but the connection is owned per-instance rather
        than shared process-global. Applies the memory limit and thread count
        from :meth:`Config.get_parquets_config`, loads the requested extensions,
        and sets the S3 region on first build. The spill (temp) directory is
        only set for on-disk databases — an in-memory test DB never spills.
        """
        if self._duckdb_client is None:
            with self._lock:
                if self._duckdb_client is None:
                    db_config = {
                        "memory_limit": self._config.memory_limit,
                        "threads": str(int(self._config.threads)),
                    }
                    if self._database != ":memory:":
                        os.makedirs(self._config.duckdb_temp_dir, exist_ok=True)
                        db_config["temp_directory"] = self._config.duckdb_temp_dir
                    db = duckdb.connect(database=self._database, config=db_config)
                    for ext in self._extensions:
                        db.execute(f"INSTALL {ext}; LOAD {ext};")
                    db.execute(f"SET GLOBAL s3_region = '{self._region}';")
                    self._duckdb_client = db
        return self._duckdb_client

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        """Run ``sql`` (optionally with bound ``params``) and return the relation.

        Each call runs on a fresh ``cursor()`` — a child connection that shares
        this client's in-memory catalog (so loaded tables are visible) but has
        its own result state. A single DuckDB connection is not safe to use
        concurrently; per-call cursors let the threadpool that serves sync API
        endpoints issue reads in parallel without stepping on each other.
        """
        cursor = self._con.cursor()
        with self._cursors_lock:
            self._active_cursors.add(cursor)
        try:
            if params is None:
                return cursor.execute(sql)
            return cursor.execute(sql, params)
        finally:
            with self._cursors_lock:
                self._active_cursors.discard(cursor)

    def create_s3_secret(self, bucket: str) -> None:
        """Create a DuckDB S3 secret scoped to ``bucket`` from boto3 credentials."""
        boto_session = boto3.Session()
        creds = boto_session.get_credentials().get_frozen_credentials()
        region = boto_session.region_name or "ap-southeast-2"

        def lit(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        def ident(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        self.execute(
            f"""
            CREATE OR REPLACE SECRET {ident(f"{bucket}_s3")} (
                TYPE S3,
                KEY_ID {lit(creds.access_key)},
                SECRET {lit(creds.secret_key)},
                SESSION_TOKEN {lit(creds.token or "")},
                REGION {lit(region)},
                SCOPE 's3://{bucket}'
            )
        """
        )

    def close(self) -> None:
        """Cancel any in-flight queries, then close the connection.

        Interrupting first keeps ``close`` from blocking on a slow query (e.g. a
        background dataset load still reading S3); the interrupted call raises in
        its own thread.
        """
        with self._cursors_lock:
            cursors = list(self._active_cursors)
        for cursor in cursors:
            try:
                cursor.interrupt()
            except Exception:
                pass
        if self._duckdb_client is not None:
            with self._lock:
                self._duckdb_client.close()
        self._duckdb_client = None

    def __enter__(self) -> ParquetDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
