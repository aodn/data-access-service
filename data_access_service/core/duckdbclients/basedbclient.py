from abc import ABC, abstractmethod
from data_access_service.config.config import IntTestConfig
from threading import Lock
from collections.abc import Sequence
from typing import Any, Optional
from data_access_service.utils.sql_utils import quote_ident, sql_literal
from data_access_service import Config

import boto3
import duckdb
import threading
import logging

# How often to emit a progress log line while a long query is running.
_PROGRESS_LOG_INTERVAL_SECONDS = 60


def _is_s3_auth_failure(exc: BaseException) -> bool:
    text = str(exc)
    return "ExpiredToken" in text or "HTTP 401" in text or "HTTP 403" in text


class DuckDBClient(ABC):
    # Serializes lazy init of the per-instance secret cache below.
    _chain_secret_init_lock = Lock()
    _logger = logging.getLogger(__name__)

    """Common interface over a DuckDB connection.

    Concrete clients own (or share) a DuckDB connection and expose a uniform
    contract: :meth:`get_instance` to obtain/lazily initialize the underlying
    handle, :meth:`execute` to run SQL, and :meth:`close` to release resources.
    Subclasses decide the connection's lifetime and concurrency model — e.g.
    :class:`PmTileDuckDBClient` shares one process-global connection, while
    :class:`SitesDuckDBClient` owns one connection and runs each call on its
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

    @staticmethod
    def _sql_preview(sql: str) -> str:
        """Short one-line form of ``sql`` for the query logs.

        CREATE SECRET carries live access keys in its text, so it is named rather
        than quoted — the logs go to CloudWatch and New Relic.
        """
        flat = " ".join(sql.split())
        if flat.upper().startswith(("CREATE SECRET", "CREATE OR REPLACE SECRET")):
            return "<CREATE SECRET, redacted>"
        return flat[:120]

    @staticmethod
    def _connect(database: str = ":memory:", config: dict | None = None):
        """Open a DuckDB database and set the timezone on that connection.

        GLOBAL, not session: queries run on cursor() children, which do not
        inherit session-scope settings.
        """
        db = duckdb.connect(database=database, config=config)
        db.execute("INSTALL httpfs; LOAD httpfs;")
        db.execute("SET GLOBAL TimeZone = 'UTC';")
        return db

    @staticmethod
    def _credential_chain_secret_sql(
        bucket: str, region: str, *, if_not_exists: bool = False
    ) -> str:
        """Credential-chain secret with no in-query refresh.

        ``REFRESH auto`` alters the secret inside the query. Cursors of one
        connection share that catalog row, so concurrent tile reads raise
        ``Catalog write-write conflict on alter``. Auth failures replace the
        secret once, outside the query, under ``_s3_chain_lock``.
        """
        exists = "IF NOT EXISTS " if if_not_exists else ""
        return (
            f"CREATE SECRET {exists}{quote_ident(f'{bucket}_s3')} ("
            "TYPE S3, PROVIDER credential_chain, "
            f"REGION {sql_literal(region)}, SCOPE {sql_literal(f's3://{bucket}')})"
        )

    def __init__(self) -> None:
        # The handle ``execute`` runs on. Subclasses replace it when they open
        # or drop their connection. The credential-chain secret cache is keyed
        # by this object, because a new connection does not keep the old secret.
        self._con: duckdb.DuckDBPyConnection | None = None
        # RLock: create_s3_secret holds this while it calls execute, and a
        # nested ExpiredToken refresh must be able to take it again.
        self._s3_chain_lock = threading.RLock()
        self._s3_chain_for: int | None = None
        self._s3_chain_secrets: set[str] = set()
        # Bumped when the task-role secret is replaced, so only the first
        # failed read of a generation writes a new secret.
        self._s3_secret_generation = 0

    def create_s3_secret(self, bucket: str) -> None:
        """Create a DuckDB S3 secret scoped to ``bucket`` from the AWS credential chain.

        The secret is NOT created with ``REFRESH auto``. That setting alters
        the secret during the query in the concurrent cursors of this
        connection then hit a catalog write-write conflict. A read that fails
        with ExpiredToken, HTTP 401, or HTTP 403 replaces the secret from
        boto3 once and retries. A later call for the same connection does not
        replace the secret on its own: ``CREATE OR REPLACE`` would drop it
        while another cursor is still reading.
        """
        boto_session = boto3.Session()

        # Not useful in testing
        if boto_session.get_credentials() is None or isinstance(
            Config.get_config(), IntTestConfig
        ):
            return
        with self._s3_chain_lock:
            # ``_con`` is the cursor or connection this client reads through.
            # Replacing it (a new cursor, or a rebuilt database) misses the
            # cache and creates the secret on that handle.
            scope = id(self._con)
            if self._s3_chain_for != scope:
                self._s3_chain_for = scope
                self._s3_chain_secrets = set()
            if bucket in self._s3_chain_secrets:
                return
            region = boto_session.region_name or "ap-southeast-2"
            self.execute("INSTALL aws; LOAD aws;")
            # No CHAIN: the default chain includes the ECS (Fargate) task role.
            # IF NOT EXISTS: a second caller must not drop a secret other
            # cursors of this connection are already reading.
            self.execute(
                DuckDBClient._credential_chain_secret_sql(
                    bucket, region, if_not_exists=True
                )
            )
            self._s3_chain_secrets.add(bucket)

    def _call_refreshing_s3(self, fn):
        """Run ``fn``. On an S3 auth failure, replace the task-role secret and run it once more.

        The secret has no ``REFRESH auto``, so DuckDB does not alter it during
        the query. ExpiredToken (HTTP 400) and HTTP 401 or 403 stay failed
        until boto3 mints a new task-role token and that token is written
        into the secret. One caller per generation does that write.
        """
        generation = self._s3_secret_generation
        try:
            return fn()
        except duckdb.HTTPException as exc:
            if not _is_s3_auth_failure(exc):
                raise
        self._refresh_expired_s3_secrets(generation)
        return fn()

    def _refresh_expired_s3_secrets(self, generation: int) -> None:
        with self._s3_chain_lock:
            if self._s3_secret_generation != generation:
                return
            self._replace_chain_secrets_from_boto()
            self._s3_secret_generation += 1

    def _replace_chain_secrets_from_boto(self) -> None:
        """Write the current boto3 credentials over each credential-chain secret.

        External-key secrets are not in ``_s3_chain_secrets`` and are left alone.
        """
        if not self._s3_chain_secrets or self._con is None:
            return
        boto_session = boto3.Session()
        credentials = boto_session.get_credentials()
        if credentials is None:
            return
        frozen = credentials.get_frozen_credentials()
        region = boto_session.region_name or "ap-southeast-2"
        token = frozen.token or ""
        for bucket in list(self._s3_chain_secrets):
            self._con.execute(
                f"CREATE OR REPLACE SECRET {quote_ident(f'{bucket}_s3')} ("
                "TYPE S3, "
                f"KEY_ID {sql_literal(frozen.access_key)}, "
                f"SECRET {sql_literal(frozen.secret_key)}, "
                f"SESSION_TOKEN {sql_literal(token)}, "
                f"REGION {sql_literal(region)}, "
                f"SCOPE {sql_literal(f's3://{bucket}')})"
            )

    def create_s3_secret_with_keys(
        self,
        bucket: str,
        access_key: str,
        secret_access_key: str,
        endpoint: Optional[str] = None,
        use_ssl: bool = True,
        region: str = "us-east-1",
    ) -> None:
        """Create a secret for a bucket the job's own role cannot read.

        For data hosted outside AODN: the keys come from the other
        organisation, and ``endpoint`` points DuckDB at their S3 service
        instead of AWS. Secrets are scoped per bucket, so this one sits
        alongside the AODN secret rather than replacing it.
        """
        clauses = [
            "TYPE S3",
            f"KEY_ID {sql_literal(access_key)}",
            f"SECRET {sql_literal(secret_access_key)}",
            # These keys are long-lived enough not to be STS session keys.
            f"SESSION_TOKEN {sql_literal('')}",
            f"REGION {sql_literal(region)}",
        ]
        if endpoint is not None:
            clauses += [
                f"ENDPOINT {sql_literal(endpoint)}",
                # Path style: an S3-compatible service usually cannot serve
                # the bucket as a subdomain of its own host.
                "URL_STYLE 'path'",
                f"USE_SSL {str(use_ssl).lower()}",
            ]
        clauses.append(f"SCOPE {sql_literal(f's3://{bucket}')}")

        self.execute(
            f"CREATE OR REPLACE SECRET {quote_ident(f'{bucket}_s3')} "
            f"({', '.join(clauses)})"
        )
