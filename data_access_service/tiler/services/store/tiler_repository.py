"""Reads batch-generated parquet slices.

``TilerParquetRepository`` wraps a :class:`TilerDuckDBClient` session and owns
the read SQL. Batch writes one parquet file per variable per timestamp
(``tiler_parquet_types.variable_parquet_path``), so a slice read is one
small ``read_parquet`` of exactly that file — no filtering, nothing to
materialize into a table.

This module also owns the shared :class:`TilerDuckDBClient` every repository
instance reads through — one connection built on first use (or eagerly by the
server lifespan via ``init_client``), mirroring
``core.estimation_index``'s ``init_client``/``close_client``.
"""

import threading

import duckdb
import numpy as np

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path

_client: TilerDuckDBClient | None = None
_client_lock = threading.Lock()


def _get_client() -> TilerDuckDBClient:
    """The module's client, building it on first use."""
    global _client
    with _client_lock:
        if _client is None:
            _client = TilerDuckDBClient()
        return _client


def init_client() -> None:
    """Build the client now, so a broken read path fails at startup rather
    than on the first tile request."""
    _get_client()


def close_client() -> None:
    """Close and clear the client this module holds (lifespan shutdown)."""
    global _client
    with _client_lock:
        client = _client
        _client = None
    if client is not None:
        client.close()


def _sql_literal(value: str) -> str:
    """Safely embed a string in SQL text (``read_parquet`` takes no bound
    parameter for the file path itself)."""
    return "'" + value.replace("'", "''") + "'"


class TilerParquetRepository:
    """Reads parquet variables through a shared DuckDB session."""

    def __init__(self, session: TilerDuckDBClient) -> None:
        self.session = session

    def fetch_variable_slice(
        self, store: str, variable: str, raw_ts: str, n_i: int, n_j: int, dtype: str
    ) -> np.ndarray:
        """One variable's dense (lat, lon) slice at ``raw_ts``, NaN outside the
        sparse rows its parquet file holds.

        Raises ``FileNotFoundError`` if the file does not exist. The sidecar
        only lists timestamps whose files batch has written, so this means
        the files were removed out from under it.
        """
        output_dir = Config.get_config().get_tiler_parquet_config().output_dir
        path = variable_parquet_path(output_dir, store, variable, raw_ts)
        try:
            cols = self.session.execute(
                f"SELECT i, j, value FROM read_parquet({_sql_literal(path)})"
            ).fetchnumpy()
        except duckdb.HTTPException as e:
            if getattr(e, "status_code", None) == 404:
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        except duckdb.IOException as e:
            if "No files found" in str(e):
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        arr = np.full((n_i, n_j), np.nan, dtype=np.dtype(dtype))
        arr[cols["i"], cols["j"]] = cols["value"]
        return arr
