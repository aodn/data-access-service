from __future__ import annotations
from __future__ import annotations

from data_access_service import Config
from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from data_access_service.models.sites_types import SitesConfig
from typing import Any
from threading import Lock
from collections.abc import Sequence

import threading
import duckdb
import os


class SitesDuckDBClient(DuckDBClient):
    """DuckDB connection for the sites repositories, used by the API and batch.

    Each :meth:`execute` runs on its own cursor, so API threads can read in
    parallel. Each repository creates the S3 secrets for its own buckets.
    """

    def __init__(self) -> None:
        super().__init__()
        # All settings come from the config (tests override
        # ``Config.get_sites_config`` to point at an in-memory DB).
        self._config: SitesConfig = Config.get_config().get_sites_config()
        self._database = self._config.duckdb_database
        self._region = self._config.region
        self._duckdb_client = None
        # Track active cursors so they can be interrupted on close.
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        self._lock = Lock()
        self._con = self.get_instance()

    def get_instance(self) -> duckdb.DuckDBPyConnection:
        """Open the connection on first use.

        The spill directory is only set for an on-disk database.
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
                    db = self._connect(database=self._database, config=db_config)
                    db.execute("INSTALL json; LOAD json;")
                    db.execute(f"SET GLOBAL s3_region = '{self._region}';")
                    self._duckdb_client = db
        return self._duckdb_client

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        """Run ``sql`` on a new cursor, so concurrent calls don't clash."""

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

    def __enter__(self) -> SitesDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
