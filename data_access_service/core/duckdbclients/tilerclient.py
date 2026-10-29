from __future__ import annotations
from data_access_service import Config
from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from threading import Lock
from data_access_service.models.tiler_types import TilerDuckDBConfig
from tempfile import TemporaryDirectory
from typing import Any, Optional
from collections.abc import Mapping, Sequence

import threading
import duckdb


class TilerDuckDBClient(DuckDBClient):
    """Reads the batch's parquet files for the tiler API.

    Each :meth:`execute` runs on its own cursor, so request threads can read
    in parallel. ``tiler_repository`` creates the S3 secret. Removes its
    spill directory on close.
    """

    def __init__(self, config: Optional[TilerDuckDBConfig] = None) -> None:
        super().__init__()
        self._config: TilerDuckDBConfig = (
            config or Config.get_config().get_tiler_api_config().duckdb
        )
        self._temp_dir = TemporaryDirectory(prefix=self._config.duckdb_temp_dir)
        self._duckdb_client: Optional[duckdb.DuckDBPyConnection] = None
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        self._lock = Lock()
        self._con = self.get_instance()

    def get_instance(self) -> duckdb.DuckDBPyConnection | None:
        """Open the connection on first use."""
        if self._duckdb_client is None:
            with self._lock:
                if self._duckdb_client is None:
                    db_config = {
                        "memory_limit": self._config.memory_limit,
                        "threads": str(int(self._config.threads)),
                        "temp_directory": self._temp_dir.name,
                        "enable_external_file_cache": (
                            self._config.enable_external_file_cache
                        ),
                        "parquet_metadata_cache": self._config.parquet_metadata_cache,
                        "enable_http_metadata_cache": (
                            self._config.enable_http_metadata_cache
                        ),
                    }
                    db = self._connect(database=":memory:", config=db_config)
                    db.execute("SET GLOBAL s3_region = 'ap-southeast-2';")
                    self._duckdb_client = db
        return self._duckdb_client

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        tables: Mapping[str, Any] | None = None,
    ):
        """Run ``sql`` on a new cursor. ``tables`` are registered on that
        cursor only, so concurrent queries can reuse names."""

        def run():
            cursor = self._con.cursor()
            with self._cursors_lock:
                self._active_cursors.add(cursor)
            try:
                for name, table in (tables or {}).items():
                    cursor.register(name, table)
                if params is None:
                    return cursor.execute(sql)
                return cursor.execute(sql, params)
            finally:
                with self._cursors_lock:
                    self._active_cursors.discard(cursor)

        return self._call_refreshing_s3(run)

    def close(self) -> None:
        """Cancel running queries, then close the connection."""
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
        self._temp_dir.cleanup()

    def __enter__(self) -> TilerDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
