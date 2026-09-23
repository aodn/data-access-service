"""Read the batch's parquet files: one file per variable per timestamp.
Holds the shared ``TilerDuckDBClient``.

Nothing reads a whole slice: each method asks DuckDB for one answer (a
range, a cell, some cells, block sums), so what comes back is sized by the
request, not the grid. ``keep`` names a table of the ``(i, j)`` cells to
count (the ocean mask); None counts every cell.
"""

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import duckdb
import numpy as np
import pyarrow as pa

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import variable_parquet_path

_client: TilerDuckDBClient | None = None
# Cell tables built on ``_client``, by the caller's key. They live and die
# with it.
_cell_tables: dict[object, str] = {}
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
        _cell_tables.clear()
    if client is not None:
        client.close()


def cell_table(key: object, cells: Callable[[], tuple[np.ndarray, np.ndarray]]) -> str:
    """The name of a table of ``cells()``'s ``(i, j)`` pairs on the shared
    client, built the first time ``key`` is asked for."""
    client = _get_client()
    with _client_lock:
        name = _cell_tables.get(key)
        if name is None:
            name = f"cells_{len(_cell_tables)}"
            TilerParquetRepository(client).create_cell_table(name, *cells())
            _cell_tables[key] = name
    return name


_READ_BATCH_ROWS = 1_000_000


def _sql_literal(value: str) -> str:
    """Quote ``value`` for SQL (``read_parquet`` can't bind its path)."""
    return "'" + value.replace("'", "''") + "'"


def _as_float(value: float | None) -> float:
    return np.nan if value is None else float(value)


@contextmanager
def _missing_as_file_not_found(path: str) -> Iterator[None]:
    try:
        yield
    except duckdb.HTTPException as e:
        if getattr(e, "status_code", None) == 404:
            raise FileNotFoundError(f"No parquet at {path!r}") from e
        raise
    except duckdb.IOException as e:
        if "No files found" in str(e):
            raise FileNotFoundError(f"No parquet at {path!r}") from e
        raise


def _index_table(name: str, index: np.ndarray, out: np.ndarray) -> pa.Table:
    """Source index -> output position, for joining onto the parquet rows."""
    return pa.table({name: index.astype(np.int32), "k": out.astype(np.int32)})


class TilerParquetRepository:
    """Reads one variable's values at one timestamp."""

    def __init__(self, session: TilerDuckDBClient) -> None:
        self.session = session

    def _path(self, store: str, variable: str, raw_ts: str) -> str:
        tiler_root_dir = Config.get_config().get_tiler_root_dir()
        return variable_parquet_path(tiler_root_dir, store, variable, raw_ts)

    @staticmethod
    def _cells(
        path: str, keep: str | None, where: str = "", columns: str = "i, j, value"
    ) -> str:
        """The file's rows as a subquery, cut to ``keep`` and ``where``."""
        semi = f" SEMI JOIN {keep} USING (i, j)" if keep else ""
        cond = f" WHERE {where}" if where else ""
        return f"(SELECT {columns} FROM read_parquet({_sql_literal(path)}){semi}{cond})"

    def create_cell_table(self, name: str, i: np.ndarray, j: np.ndarray) -> None:
        """A table ``name`` of these ``(i, j)`` cells, for ``keep``."""
        self.session.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT i, j FROM cells",
            tables={
                "cells": pa.table({"i": i.astype(np.int32), "j": j.astype(np.int32)})
            },
        )

    def fetch_value_range(
        self, store: str, variable: str, raw_ts: str, keep: str | None = None
    ) -> tuple[float, float]:
        """The values' (min, max), NaN if there are none. Raises
        FileNotFoundError if the file is missing.

        From the footer when every cell counts, so no values are read. The
        footer can't know about ``keep``, and a row group written without
        statistics can't answer either; both scan the values instead."""
        path = self._path(store, variable, raw_ts)
        with _missing_as_file_not_found(path):
            if keep is None:
                row = self.session.execute(
                    "SELECT min(stats_min_value::DOUBLE), "
                    "max(stats_max_value::DOUBLE), any_value(type), "
                    "count(*) FILTER (stats_min_value IS NULL "
                    "AND row_group_num_rows > 0) "
                    f"FROM parquet_metadata({_sql_literal(path)}) "
                    "WHERE path_in_schema = 'value'"
                ).fetchone()
                lo, hi, physical, unstated = row
                if not unstated:
                    # The footer holds each value's shortest text; round it
                    # back to the stored type so it matches the values.
                    as_stored = np.float32 if physical == "FLOAT" else np.float64
                    return (
                        _as_float(None if lo is None else as_stored(lo)),
                        _as_float(None if hi is None else as_stored(hi)),
                    )
            lo, hi = self.session.execute(
                f"SELECT min(value), max(value) FROM {self._cells(path, keep)}"
            ).fetchone()
        return _as_float(lo), _as_float(hi)

    def fetch_point(
        self,
        store: str,
        variable: str,
        raw_ts: str,
        i: int,
        j: int,
        keep: str | None = None,
    ) -> float:
        """The value at cell ``(i, j)``, NaN if it has none."""
        path = self._path(store, variable, raw_ts)
        with _missing_as_file_not_found(path):
            row = self.session.execute(
                f"SELECT value FROM {self._cells(path, keep, 'i = ? AND j = ?')}",
                [int(i), int(j)],
            ).fetchone()
        return np.nan if row is None else float(row[0])

    def fetch_gather(
        self,
        store: str,
        variable: str,
        raw_ts: str,
        rows: np.ndarray,
        cols: np.ndarray,
        dtype: np.dtype,
        keep: str | None = None,
    ) -> np.ndarray:
        """A dense ``(len(rows), len(cols))`` array of just these rows and
        columns (any order), NaN where there is no value.

        Only those cells come back, so a sampled window costs what it
        returns, not what it spans. Built batch by batch, so the rows are
        never all in memory at once."""
        out = np.full((len(rows), len(cols)), np.nan, dtype=dtype)
        if not len(rows) or not len(cols):
            return out
        path = self._path(store, variable, raw_ts)
        # The range lets DuckDB skip row groups; the joins pick the cells.
        where = (
            f"i BETWEEN {int(rows.min())} AND {int(rows.max())} "
            f"AND j BETWEEN {int(cols.min())} AND {int(cols.max())}"
        )
        with _missing_as_file_not_found(path):
            reader = self.session.execute(
                "SELECT r.k AS r, c.k AS c, p.value "
                f"FROM {self._cells(path, keep, where)} p "
                "JOIN sel_i r ON p.i = r.i JOIN sel_j c ON p.j = c.j",
                tables={
                    "sel_i": _index_table("i", rows, np.arange(len(rows))),
                    "sel_j": _index_table("j", cols, np.arange(len(cols))),
                },
            ).to_arrow_reader(_READ_BATCH_ROWS)
            for batch in reader:
                r, c, value = (
                    col.to_numpy(zero_copy_only=False) for col in batch.columns
                )
                out[r, c] = value
        return out

    def fetch_block_sums(
        self,
        store: str,
        variable: str,
        raw_ts: str,
        row_edges: np.ndarray,
        col_edges: np.ndarray,
        keep: str | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """The sum and count of the values in each block the edges mark out:
        rows ``row_edges[k]:row_edges[k + 1]`` by columns alike.

        Each cell's block comes from the edges here, never from arithmetic
        in SQL, so the blocks are exactly the ones the caller laid out."""
        out_rows, out_cols = len(row_edges) - 1, len(col_edges) - 1
        total = np.zeros((out_rows, out_cols), dtype=np.float64)
        count = np.zeros((out_rows, out_cols), dtype=np.int64)
        r0, r1 = int(row_edges[0]), int(row_edges[-1])
        c0, c1 = int(col_edges[0]), int(col_edges[-1])
        if r1 <= r0 or c1 <= c0:
            return total, count
        rows = np.arange(r0, r1)
        cols = np.arange(c0, c1)
        path = self._path(store, variable, raw_ts)
        where = f"i >= {r0} AND i < {r1} AND j >= {c0} AND j < {c1}"
        with _missing_as_file_not_found(path):
            result = self.session.execute(
                "SELECT r.k AS r, c.k AS c, sum(p.value::DOUBLE) AS s, "
                "count(*) AS n "
                f"FROM {self._cells(path, keep, where)} p "
                "JOIN blk_i r ON p.i = r.i JOIN blk_j c ON p.j = c.j "
                "GROUP BY ALL",
                tables={
                    "blk_i": _index_table(
                        "i", rows, np.searchsorted(row_edges, rows, side="right") - 1
                    ),
                    "blk_j": _index_table(
                        "j", cols, np.searchsorted(col_edges, cols, side="right") - 1
                    ),
                },
            ).to_arrow_table()
        r = result["r"].to_numpy()
        c = result["c"].to_numpy()
        total[r, c] = result["s"].to_numpy()
        count[r, c] = result["n"].to_numpy()
        return total, count
