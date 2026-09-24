"""A ``GridSource`` that keeps nothing in memory: each read is a DuckDB
query against the variable's parquet, sized by what it returns."""

import functools
from dataclasses import dataclass

import numpy as np

from data_access_service.tiler.services.store.sparse_grid import (
    block_means,
    window_edges,
)
from data_access_service.tiler.services.store.tiler_repository import (
    TilerParquetRepository,
    _get_client,
)


@functools.lru_cache(maxsize=65536)
def value_range(
    store: str, variable: str, raw_ts: str, keep: str | None
) -> tuple[float, float]:
    """(min, max) of one variable at one timestamp. Batch never rewrites a
    file, so a range never goes stale; failures aren't cached."""
    repo = TilerParquetRepository(_get_client())
    return repo.fetch_value_range(store, variable, raw_ts, keep)


@dataclass(frozen=True)
class ParquetGridSource:
    store: str
    variable: str
    raw_ts: str
    n_i: int
    n_j: int
    dtype: np.dtype
    # Table of the (i, j) cells to count (the ocean mask); None for all.
    keep: str | None = None

    def _repo(self) -> TilerParquetRepository:
        return TilerParquetRepository(_get_client())

    @property
    def vmin(self) -> float:
        return value_range(self.store, self.variable, self.raw_ts, self.keep)[0]

    @property
    def vmax(self) -> float:
        return value_range(self.store, self.variable, self.raw_ts, self.keep)[1]

    def value_at(self, i: int, j: int) -> float:
        return self._repo().fetch_point(
            self.store, self.variable, self.raw_ts, i, j, self.keep
        )

    def gather(self, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
        return self._repo().fetch_gather(
            self.store,
            self.variable,
            self.raw_ts,
            np.asarray(rows),
            np.asarray(cols),
            self.dtype,
            self.keep,
        )

    def aggregate(
        self, rows: slice, cols: slice, out_rows: int, out_cols: int
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        return self.aggregate_blocks(*window_edges(rows, cols, out_rows, out_cols))

    def aggregate_blocks(
        self, row_edges: np.ndarray, col_edges: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        total, count = self._repo().fetch_block_sums(
            self.store, self.variable, self.raw_ts, row_edges, col_edges, self.keep
        )
        return block_means(total, count), row_edges, col_edges
