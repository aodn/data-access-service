"""Read the batch's parquet files: one file per variable per timestamp.
Holds the shared ``TilerDuckDBClient``."""

import threading

import duckdb
import numpy as np

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path
from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    index_dtype,
)

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


_SQL_INT = {np.dtype(np.int16): "SMALLINT", np.dtype(np.int32): "INTEGER"}

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
    ) -> SparseGrid:
        """One variable's (lat, lon) grid at ``raw_ts``, as CSR. Raises
        FileNotFoundError if the file is missing."""
        output_dir = Config.get_config().get_tiler_output_dir()
        path = variable_parquet_path(output_dir, store, variable, raw_ts)
        source = f"read_parquet({_sql_literal(path)})"
        try:
            # The row count comes from the file footer; it sizes the arrays.
            (n,) = self.session.execute(f"SELECT count(*) FROM {source}").fetchone()
            grid = self._read_csr(source, n, n_i, n_j, dtype)
            if grid is None:
                # Batch writes rows sorted by (i, j); sort here if they aren't.
                grid = self._read_csr(source, n, n_i, n_j, dtype, " ORDER BY i, j")
        except duckdb.HTTPException as e:
            if getattr(e, "status_code", None) == 404:
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        except duckdb.IOException as e:
            if "No files found" in str(e):
                raise FileNotFoundError(f"No parquet at {path!r}") from e
            raise
        assert grid is not None
        return grid

    def _read_csr(
        self,
        source: str,
        n: int,
        n_i: int,
        n_j: int,
        dtype: str,
        order_by: str = "",
    ) -> SparseGrid | None:
        """Stream the rows batch by batch into the CSR arrays, so only one
        copy is ever in memory. None if they aren't sorted by (i, j)."""
        # Cast the indexes to the smallest type that fits (int16 for most grids).
        i_type = _SQL_INT[index_dtype(n_i)]
        j_type = _SQL_INT[index_dtype(n_j)]
        reader = self.session.execute(
            f"SELECT i::{i_type} AS i, j::{j_type} AS j, value FROM {source}{order_by}"
        ).to_arrow_reader(_READ_BATCH_ROWS)

        j_out = np.empty(n, dtype=index_dtype(n_j))
        value_out = np.empty(n, dtype=np.dtype(dtype))
        counts = np.zeros(n_i, dtype=np.int64)
        pos = 0
        last = (-1, -1)
        for batch in reader:
            i, j, value = (c.to_numpy(zero_copy_only=False) for c in batch.columns)
            if not len(i):
                continue
            if not _sorted(i, j, last):
                return None
            j_out[pos : pos + len(i)] = j
            value_out[pos : pos + len(i)] = value
            counts += np.bincount(i, minlength=n_i)
            pos += len(i)
            last = (int(i[-1]), int(j[-1]))
        if pos != n:
            raise RuntimeError(f"Read {pos} rows from {source}, expected {n}")

        row_ptr = np.zeros(n_i + 1, dtype=np.int64)
        np.cumsum(counts, out=row_ptr[1:])
        return SparseGrid(n_i, n_j, row_ptr, j_out, value_out)


def _sorted(i: np.ndarray, j: np.ndarray, last: tuple[int, int]) -> bool:
    """Whether the (i, j) rows strictly increase, carrying on from ``last``."""
    i = np.concatenate(([last[0]], i.astype(np.int32)))
    j = np.concatenate(([last[1]], j.astype(np.int32)))
    di = np.diff(i)
    return bool(np.all((di > 0) | ((di == 0) & (np.diff(j) > 0))))
