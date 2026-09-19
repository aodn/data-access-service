"""Read the batch's parquet files: one file per variable per timestamp.
Holds the shared ``TilerDuckDBClient``."""

import threading

import duckdb
import numpy as np

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path

_client: TilerDuckDBClient | None = None
_client_lock = threading.Lock()


def _get_client() -> TilerDuckDBClient:
    """The shared client, built on first use."""
    global _client
    with _client_lock:
        if _client is None:
            _client = TilerDuckDBClient()
        return _client


def init_client() -> None:
    """Build the client at startup, so a broken setup fails early."""
    _get_client()


def close_client() -> None:
    """Close the shared client (shutdown)."""
    global _client
    with _client_lock:
        client = _client
        _client = None
    if client is not None:
        client.close()


_READ_BATCH_ROWS = 1_000_000


def _sql_literal(value: str) -> str:
    """Quote ``value`` for SQL (``read_parquet`` can't bind its path)."""
    return "'" + value.replace("'", "''") + "'"


class TilerParquetRepository:
    """Reads variable slices from parquet."""

    def __init__(self, session: TilerDuckDBClient) -> None:
        self.session = session

    def fetch_variable_slice(
        self, store: str, variable: str, raw_ts: str, n_i: int, n_j: int, dtype: str
    ) -> np.ndarray:
        """One variable's dense (lat, lon) grid at ``raw_ts``, NaN where the
        file has no row. Raises FileNotFoundError if the file is missing."""
        output_dir = Config.get_config().get_tiler_output_dir()
        path = variable_parquet_path(output_dir, store, variable, raw_ts)
        arr = np.full((n_i, n_j), np.nan, dtype=np.dtype(dtype))
        try:
            reader = self.session.execute(
                f"SELECT i, j, value FROM read_parquet({_sql_literal(path)})"
            ).to_arrow_reader(_READ_BATCH_ROWS)
            # Batch by batch, so the rows are never all in memory at once.
            for batch in reader:
                i, j, value = (c.to_numpy(zero_copy_only=False) for c in batch.columns)
                arr[i, j] = value
        except duckdb.HTTPException as e:
            if getattr(e, "status_code", None) == 404:
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        except duckdb.IOException as e:
            if "No files found" in str(e):
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        return arr
