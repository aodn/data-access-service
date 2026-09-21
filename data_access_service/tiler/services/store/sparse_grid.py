"""One variable's slice in CSR form: only the cells with a value are kept.

Row ``i``'s cells are ``j[row_ptr[i]:row_ptr[i + 1]]`` (sorted), with values
at the same positions in ``value``.
"""

from dataclasses import dataclass, field

import numpy as np

# int16 holds any index below this; bigger grids keep int32.
_INT16_LIMIT = np.iinfo(np.int16).max + 1


def index_dtype(n: int) -> np.dtype:
    """The smallest dtype for indexes ``0..n-1``."""
    return np.dtype(np.int16 if n <= _INT16_LIMIT else np.int32)


@dataclass(frozen=True)
class SparseGrid:
    n_i: int
    n_j: int
    row_ptr: np.ndarray
    j: np.ndarray
    value: np.ndarray
    # Min/max of ``value`` (NaN if empty), worked out once when built.
    vmin: float = field(init=False)
    vmax: float = field(init=False)

    def __post_init__(self) -> None:
        empty = self.value.size == 0
        vmin = np.nan if empty else float(np.fmin.reduce(self.value))
        vmax = np.nan if empty else float(np.fmax.reduce(self.value))
        object.__setattr__(self, "vmin", vmin)
        object.__setattr__(self, "vmax", vmax)

    @classmethod
    def from_rows(
        cls, i: np.ndarray, j: np.ndarray, value: np.ndarray, n_i: int, n_j: int
    ) -> "SparseGrid":
        """Build from ``(i, j, value)`` rows, sorting them if needed."""
        di = np.diff(i)
        if not np.all((di > 0) | ((di == 0) & (np.diff(j) > 0))):
            order = np.lexsort((j, i))
            i, j, value = i[order], j[order], value[order]
        row_ptr = np.searchsorted(i, np.arange(n_i + 1)).astype(np.int64)
        return cls(n_i, n_j, row_ptr, j, value)

    @property
    def dtype(self) -> np.dtype:
        return self.value.dtype

    def value_at(self, i: int, j: int) -> float:
        """The value at cell ``(i, j)``, NaN if it has none."""
        a, b = self.row_ptr[i], self.row_ptr[i + 1]
        k = a + int(np.searchsorted(self.j[a:b], j))
        if k < b and self.j[k] == j:
            return float(self.value[k])
        return np.nan

    def gather(self, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
        """A dense ``(len(rows), len(cols))`` array of just these rows and
        columns (any order), NaN where there is no value."""
        out = np.full((len(rows), len(cols)), np.nan, dtype=self.dtype)
        # Source column -> output column, -1 if not wanted.
        col_map = np.full(self.n_j, -1, dtype=np.intp)
        col_map[cols] = np.arange(len(cols))
        for k, r in enumerate(rows):
            a, b = self.row_ptr[r], self.row_ptr[r + 1]
            if a == b:
                continue
            out_cols = col_map[self.j[a:b]]
            wanted = out_cols >= 0
            out[k, out_cols[wanted]] = self.value[a:b][wanted]
        return out

    def aggregate(
        self,
        rows: slice,
        cols: slice,
        out_rows: int,
        out_cols: int,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """The mean of every cell in ``rows`` x ``cols``, on an
        ``(out_rows, out_cols)`` grid of equal blocks.

        ``rows`` and ``cols`` are contiguous ranges; only their start and stop
        are read. Asking for more blocks than there are cells gives one block
        per cell.

        Every cell counts, not one sample per block, so the result is the
        area mean rather than whichever cell a step landed on. The dense
        window is never built: rows of a block sit together in the CSR, so
        each block is one slice and two ``bincount``s.

        Returns ``(mean, row_edges, col_edges)``; ``mean`` is NaN where a
        block holds no values, and the edges are the source indexes each
        block spans, for working out its coordinates.
        """
        r0, r1 = rows.start, rows.stop
        c0, c1 = cols.start, cols.stop
        out_rows = max(1, min(out_rows, r1 - r0))
        out_cols = max(1, min(out_cols, c1 - c0))
        row_edges = np.linspace(r0, r1, out_rows + 1).astype(np.intp)
        col_edges = np.linspace(c0, c1, out_cols + 1).astype(np.intp)
        return self.aggregate_blocks(row_edges, col_edges)

    def aggregate_blocks(
        self, row_edges: np.ndarray, col_edges: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """``aggregate`` over blocks the caller lays out.

        Pass edges that depend only on the output grid, not on the window
        asked for, and neighbouring windows agree on every shared block -
        which is what keeps stitched tiles from showing a seam.
        """
        out_rows = len(row_edges) - 1
        out_cols = len(col_edges) - 1
        c0, c1 = int(col_edges[0]), int(col_edges[-1])

        # Source column -> output block, -1 outside the window.
        col_block = np.full(self.n_j, -1, dtype=np.intp)
        inside = np.arange(c0, c1)
        col_block[inside] = np.searchsorted(col_edges, inside, side="right") - 1

        total = np.zeros((out_rows, out_cols), dtype=np.float64)
        count = np.zeros((out_rows, out_cols), dtype=np.int64)
        for k in range(out_rows):
            a = int(self.row_ptr[row_edges[k]])
            b = int(self.row_ptr[row_edges[k + 1]])
            if a == b:
                continue
            block = col_block[self.j[a:b]]
            wanted = block >= 0
            if not wanted.any():
                continue
            hit = block[wanted]
            total[k] = np.bincount(
                hit, weights=self.value[a:b][wanted], minlength=out_cols
            )
            count[k] = np.bincount(hit, minlength=out_cols)

        with np.errstate(invalid="ignore", divide="ignore"):
            mean = np.where(count > 0, total / count, np.nan)
        return mean.astype(np.float32), row_edges, col_edges

    def keep(self, valid: np.ndarray) -> "SparseGrid":
        """Only the cells where the dense (n_i, n_j) mask ``valid`` is True."""
        keep = np.empty(self.value.size, dtype=bool)
        row_ptr = np.zeros(self.n_i + 1, dtype=np.int64)
        for r in range(self.n_i):
            a, b = self.row_ptr[r], self.row_ptr[r + 1]
            keep[a:b] = valid[r, self.j[a:b]]
            row_ptr[r + 1] = row_ptr[r] + np.count_nonzero(keep[a:b])
        return SparseGrid(self.n_i, self.n_j, row_ptr, self.j[keep], self.value[keep])


@dataclass(frozen=True)
class SparseSlice:
    """A store's variables at one timestamp, with the grid's coordinates."""

    lat: np.ndarray
    lon: np.ndarray
    grids: dict[str, SparseGrid]
    attrs: dict[str, dict]

    def bounds(self) -> tuple[float, float, float, float]:
        """(lon_min, lon_max, lat_min, lat_max)."""
        return (
            float(self.lon.min()),
            float(self.lon.max()),
            float(self.lat.min()),
            float(self.lat.max()),
        )
