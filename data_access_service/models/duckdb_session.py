"""The DuckDB connection.

``DuckDBSession`` is the only object that touches the connection. A
:class:`~repository.ParquetRepository` binds one session to one Parquet dataset.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Sequence
from typing import Any

import boto3
import duckdb


class DuckDBSession:
    """Owns the DuckDB connection and its extension/region configuration.

    Loads the requested extensions and sets the S3 region. The session does
    not decide *which* buckets get credentials — each
    :class:`~repository.ParquetRepository` calls :meth:`create_s3_secret` for
    its own buckets on construction (see ``ParquetRepository._configure_s3``).
    The session owns the secret SQL and the boto3 plumbing; the repository owns
    the bucket choice. Usable as a context manager.

    """

    def __init__(
        self,
        region: str = "ap-southeast-2",
        *,
        database: str = ":memory:",
        extensions: Iterable[str] = ("httpfs", "json"),
    ) -> None:
        self.conn = duckdb.connect(database=database)
        # Track active cursors so they can be interrupted on close; 
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        for ext in extensions:
            self.conn.execute(f"INSTALL {ext}; LOAD {ext};")
        
        self.conn.execute(f"SET GLOBAL s3_region = '{region}';")

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        """Run ``sql`` (optionally with bound ``params``) and return the relation.

        Each call runs on a fresh ``cursor()`` — a child connection that shares
        this session's in-memory catalog (so loaded tables are visible) but has
        its own result state. A single DuckDB connection is not safe to use
        concurrently; per-call cursors let the threadpool that serves sync API
        endpoints issue reads in parallel without stepping on each other.
        """
        cursor = self.conn.cursor()
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
        """Create a DuckDB S3 secret scoped to ``bucket`` from boto3 credentials.

        """
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
        self.conn.close()

    def __enter__(self) -> DuckDBSession:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
