from __future__ import annotations

from collections.abc import Sequence
from threading import Lock
from typing import Any

import duckdb
import threading

from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from data_access_service.models.data_index_types import DataIndexConfig
from data_access_service.utils.sql_utils import quote_ident, sql_literal

import boto3


class DataIndexDuckDBClient(DuckDBClient):
    """Reads the collection-mapping table in the data-index S3 Tables bucket.

    One in-memory connection. Each :meth:`execute` uses its own cursor.
    The catalog is attached once. The S3 secret is unscoped because table
    files do not live under ``s3://data-index``.
    """

    _SECRET_NAME = "data_index_s3"

    def __init__(self) -> None:
        super().__init__()
        self._config: DataIndexConfig = Config.get_config().get_data_index_config()
        self._duckdb_client: duckdb.DuckDBPyConnection | None = None
        self._active_cursors: set[Any] = set()
        self._cursors_lock = threading.Lock()
        self._lock = Lock()
        self._attached = False
        self._con = self.get_instance()

    def get_instance(self) -> duckdb.DuckDBPyConnection | None:
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
                    db.execute(
                        f"SET GLOBAL s3_region = {sql_literal(self._config.region)};"
                    )
                    db.execute("INSTALL iceberg; LOAD iceberg;")
                    self._duckdb_client = db
                    self._con = db
                    self._ensure_secret()
                    self._attach_catalog()
        return self._duckdb_client

    @property
    def qualified_table(self) -> str:
        """``catalog.namespace.table`` for the live collection-mapping table."""
        cfg = self._config
        return ".".join(
            quote_ident(part)
            for part in (cfg.catalog_name, cfg.namespace, cfg.table_name)
        )

    def _ensure_secret(self) -> None:
        """Unscoped S3 secret from the current boto3 credentials.

        S3 Tables objects are not under ``s3://data-index``, so the secret has
        no SCOPE. Frozen keys are written directly: DuckDB's credential_chain
        provider rejects an SSO profile at CREATE SECRET time.
        """
        if isinstance(Config.get_config(), IntTestConfig):
            return
        if self._con is None or boto3.Session().get_credentials() is None:
            return
        self._con.execute("INSTALL aws; LOAD aws;")
        self._replace_chain_secrets_from_boto()

    def _attach_catalog(self) -> None:
        if self._attached or isinstance(Config.get_config(), IntTestConfig):
            return
        alias = quote_ident(self._config.catalog_name)
        self.execute(
            f"ATTACH {sql_literal(self._config.table_bucket_arn)} AS {alias} ("
            "TYPE iceberg, ENDPOINT_TYPE s3_tables)"
        )
        self._attached = True

    def _replace_chain_secrets_from_boto(self) -> None:
        """Refresh the unscoped secret. The base class only rewrites scoped ones."""
        if self._con is None or isinstance(Config.get_config(), IntTestConfig):
            return
        credentials = boto3.Session().get_credentials()
        if credentials is None:
            return
        frozen = credentials.get_frozen_credentials()
        token = frozen.token or ""
        # No SCOPE: table data lives in the S3 Tables warehouse bucket,
        # which is not ``s3://data-index``.
        self._con.execute(
            f"CREATE OR REPLACE SECRET {quote_ident(self._SECRET_NAME)} ("
            "TYPE S3, "
            f"KEY_ID {sql_literal(frozen.access_key)}, "
            f"SECRET {sql_literal(frozen.secret_key)}, "
            f"SESSION_TOKEN {sql_literal(token)}, "
            f"REGION {sql_literal(self._config.region)})"
        )

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        """Run ``sql`` on a new cursor so concurrent reads do not share one cursor."""

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
        self._attached = False

    def __enter__(self) -> DataIndexDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
