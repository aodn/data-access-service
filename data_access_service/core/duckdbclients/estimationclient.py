from __future__ import annotations
from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from data_access_service.models.estimation_types import EstimationReadDuckDBConfig
from typing import Any, Optional
from threading import Lock
from collections.abc import Sequence

import duckdb
import threading


class EstimationDuckDBClient(DuckDBClient):
    """Reads the pre-built estimation index, on a connection of its own."""

    def __init__(self, config: EstimationReadDuckDBConfig) -> None:
        super().__init__()
        self._config = config
        self._duckdb_client: Optional[duckdb.DuckDBPyConnection] = None
        # Track active cursors so they can be interrupted on close.
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        self._lock = Lock()
        self._con = self.get_instance()

    def get_instance(self) -> duckdb.DuckDBPyConnection | None:
        """Initialize this client's owned in-memory connection if it does not exist.

        Only httpfs is loaded: the index parquet is read from S3, and the JSON
        sidecar beside it is fetched with boto3 (see
        ``estimation_index.load_sidecar``), not by DuckDB.
        """
        if self._duckdb_client is None:
            with self._lock:
                if self._duckdb_client is None:
                    db = self._connect(
                        database=":memory:",
                        config={
                            "memory_limit": self._config.memory_limit,
                            "threads": str(int(self._config.threads)),
                        },
                    )
                    db.execute(f"SET GLOBAL s3_region = '{self._config.region}';")
                    self._duckdb_client = db
        return self._duckdb_client

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        """Run ``sql`` (optionally with bound ``params``) on a fresh cursor."""

        def run():
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

        return self._call_refreshing_s3(run)

    def close(self) -> None:
        """Cancel any in-flight queries, then close the connection."""
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

    def __enter__(self) -> EstimationDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
