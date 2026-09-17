"""Reads one store's batch-generated parquet slices.

``TilerParquetRepository`` binds a :class:`TilerDuckDBClient` session to one
zarr-derived store's parquet directory (written by
``batch.tiler.parquet_generator``) and owns the read SQL — mirroring
``sites.sites_repository.ParquetRepository``'s split between a shared DuckDB
session and a dataset-bound repository.

It differs from the sites repository in one way that matches how the data is
laid out: there's no fixed, enumerable set of dataset subclasses to declare
(tiler stores are discovered at runtime from ``root_metadata.json``/each
store's ``metadata.json``, see [[store.registry]]), and nothing to
materialize into a table — each variable already lives in its own small
parquet file, so every read is a direct ``read_parquet`` point query against
the exact file for the requested variable and timestamp.

This module also owns the shared :class:`TilerDuckDBClient` every repository
instance reads through — one connection built on first use (or eagerly by the
server lifespan via ``init_client``), mirroring
``core.estimation_index``'s ``init_client``/``close_client``.
"""

import os
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


class TilerParquetRepository:
    """Reads a single store's parquet variables, keyed by ``dataset_stem``."""

    def __init__(self, session: TilerDuckDBClient, dataset_stem: str) -> None:
        self.session = session
        self.dataset_stem = dataset_stem

    def _variable_parquet_path(self, variable: str) -> str:
        output_dir = Config.get_config().get_tiler_parquet_config().output_dir
        return os.path.join(output_dir, self.dataset_stem, f"{variable}.parquet")

    def fetch_variable_slice(
        self, variable: str, raw_ts: str, n_i: int, n_j: int, dtype: str
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
        path = self._variable_parquet_path(variable)
        rows = self.session.execute(
            f"SELECT i, j, value FROM read_parquet({_sql_literal(path)}) "
            "WHERE timestamp = ?",
            [raw_ts],
        ).fetchall()
        if not rows:
            raise FileNotFoundError(
                f"No data for variable {variable!r} at {raw_ts!r} in "
                f"store {self.dataset_stem!r} — this timestamp was not "
                "converted to parquet"
            )
        arr = np.full((n_i, n_j), np.nan, dtype=np.dtype(dtype))
        for i, j, value in rows:
            arr[i, j] = value
        return arr
