"""Reads batch-generated parquet slices.

``TilerParquetRepository`` wraps a :class:`TilerDuckDBClient` session and owns
the read SQL. Each variable's ``TilerVariableMetadata.parquet_path`` already
names its exact file (relative to ``TilerParquetConfig.output_dir``), so a
read is a direct ``read_parquet`` point query — nothing to materialize into a
table, no per-store binding needed.

This module also owns the shared :class:`TilerDuckDBClient` every repository
instance reads through — one connection built on first use (or eagerly by the
server lifespan via ``init_client``), mirroring
``core.estimation_index``'s ``init_client``/``close_client``.
"""

import threading

import numpy as np

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient

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


def _resolve_path(parquet_path: str) -> str:
    output_dir = Config.get_config().get_tiler_parquet_config().output_dir
    return f"{output_dir.rstrip('/')}/{parquet_path}"


class TilerParquetRepository:
    """Reads parquet variables through a shared DuckDB session."""

    def __init__(self, session: TilerDuckDBClient) -> None:
        self.session = session

    def fetch_variable_slice(
        self, parquet_path: str, raw_ts: str, n_i: int, n_j: int, dtype: str
    ) -> np.ndarray:
        """One variable's dense (lat, lon) slice at ``raw_ts``, NaN outside the
        sparse rows the parquet actually holds for that timestamp.

        Raises ``FileNotFoundError`` if the parquet has zero rows for
        ``raw_ts`` — a real timestamp always has at least some valid (ocean)
        cells, so zero rows means this instant was never converted (e.g. a
        partial/sampled batch backfill whose ``metadata.json`` still lists the
        store's full history — see ``batch.tiler.parquet_generator``), not
        that every cell happens to be masked.
        """
        path = _resolve_path(parquet_path)
        rows = self.session.execute(
            f"SELECT i, j, value FROM read_parquet({_sql_literal(path)}) "
            "WHERE timestamp = ?",
            [raw_ts],
        ).fetchall()
        if not rows:
            raise FileNotFoundError(
                f"No data for {parquet_path!r} at {raw_ts!r} — this timestamp "
                "was not converted to parquet"
            )
        arr = np.full((n_i, n_j), np.nan, dtype=np.dtype(dtype))
        for i, j, value in rows:
            arr[i, j] = value
        return arr
