"""Resample a window of the slice to the LOD grid, and numba kernels to
normalise it to integers."""

import logging
import threading

import numpy as np

from data_access_service.tiler.services.store.sparse_grid import SparseGrid

logger = logging.getLogger(__name__)

# Numba's default threading layer isn't thread-safe: two parallel kernels
# running at once can corrupt output or crash. Held per kernel call.
_PARALLEL_KERNEL_LOCK = threading.Lock()


try:
    from numba import njit, prange

    _HAS_NUMBA = True

    # No 'nnan' in fastmath, so np.isnan works here.
    @njit(
        parallel=True,
        cache=True,
        fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    )
    def _numba_normalize_uint32(
        arr: np.ndarray, lo: float, hi: float, out_max: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """float32 -> uint32, plus a valid mask (1 where not NaN)."""
        h, w = arr.shape
        out = np.empty((h, w), dtype=np.uint32)
        valid = np.empty((h, w), dtype=np.uint8)
        span = hi - lo if hi != lo else 1.0
        scale = (1.0 / span) * out_max
        out_max_f = float(out_max)
        for i in prange(h):
            for j in range(w):
                v = arr[i, j]
                if np.isnan(v):
                    out[i, j] = np.uint32(0)
                    valid[i, j] = np.uint8(0)
                else:
                    val = (v - lo) * scale
                    if val < 0.0:
                        val = 0.0
                    elif val > out_max_f:
                        val = out_max_f
                    out[i, j] = np.uint32(val)
                    valid[i, j] = np.uint8(1)
        return out, valid

    @njit(
        parallel=True,
        cache=True,
        fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    )
    def _numba_normalize_uint8(
        arr: np.ndarray, lo: float, hi: float, out_max: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """The uint8 version, for variable pairs."""
        h, w = arr.shape
        out = np.empty((h, w), dtype=np.uint8)
        valid = np.empty((h, w), dtype=np.uint8)
        span = hi - lo if hi != lo else 1.0
        scale = (1.0 / span) * out_max
        out_max_f = float(out_max)
        for i in prange(h):
            for j in range(w):
                v = arr[i, j]
                if np.isnan(v):
                    out[i, j] = np.uint8(0)
                    valid[i, j] = np.uint8(0)
                else:
                    val = (v - lo) * scale
                    if val < 0.0:
                        val = 0.0
                    elif val > out_max_f:
                        val = out_max_f
                    out[i, j] = np.uint8(val)
                    valid[i, j] = np.uint8(1)
        return out, valid

except ImportError:  # pragma: no cover
    _HAS_NUMBA = False
    logger.warning("numba unavailable; normalising with numpy (slower)")


def _positions(n_src: int, n_out: int, start: int, stop: int) -> np.ndarray:
    """Where output pixels ``start:stop`` fall on the source axis (corners
    aligned)."""
    scale = (n_src - 1.0) / (n_out - 1.0) if n_out > 1 else 0.0
    return np.arange(start, stop) * scale


# Source cells per output pixel at which sampling four of them starts to
# throw away more than it keeps, so the window is averaged instead.
_AGGREGATE_PITCH = 2.0


def _block_edges(n_src: int, n_out: int, start: int, stop: int) -> np.ndarray:
    """Where each output pixel's block begins and ends on the source axis.

    Each pixel owns the half-step either side of where it samples. An edge
    depends only on that pixel's index, never on the window asked for, so
    two tiles that share a block compute the same mean for it.
    """
    scale = (n_src - 1.0) / (n_out - 1.0) if n_out > 1 else 0.0
    edges = np.rint((np.arange(start, stop + 1) - 0.5) * scale).astype(np.intp)
    return np.clip(edges, 0, n_src)


def aggregate_window(
    grid: SparseGrid,
    total_h: int,
    total_w: int,
    rows: tuple[int, int],
    cols: tuple[int, int],
    *,
    flip: bool,
) -> np.ndarray | None:
    """``resample_window``'s window as block means, or None if sampling suits.

    Averaging every cell an output pixel covers keeps the cells that
    sampling steps over - on a sparse grid that is most of them - and it
    never needs the window at the source's own resolution. None when the
    pixels are close enough to the source's resolution that there is nothing
    to average.
    """
    pitch = max(
        (grid.n_i - 1.0) / (total_h - 1.0) if total_h > 1 else 0.0,
        (grid.n_j - 1.0) / (total_w - 1.0) if total_w > 1 else 0.0,
    )
    if pitch < _AGGREGATE_PITCH:
        return None

    row_edges = _block_edges(grid.n_i, total_h, *rows)
    col_edges = _block_edges(grid.n_j, total_w, *cols)
    if np.any(np.diff(row_edges) < 1) or np.any(np.diff(col_edges) < 1):
        return None  # a block with no cells of its own; sampling suits better

    if flip:
        # Rows count north to south, so mirror the edges and the result.
        row_edges = (grid.n_i - row_edges)[::-1]
    mean, _, _ = grid.aggregate_blocks(row_edges, col_edges)
    return mean[::-1] if flip else mean


def _gather(
    grid: SparseGrid,
    flip: bool,
    row_sets: list[np.ndarray],
    col_sets: list[np.ndarray],
) -> tuple[np.ndarray, list[np.ndarray], list[np.ndarray]]:
    """Build only the source rows/cols in these sets, and remap each set to
    index the built array. Rows count north to south (flipped if ``flip``)."""
    rows = np.unique(np.concatenate(row_sets))
    cols = np.unique(np.concatenate(col_sets))
    src_rows = grid.n_i - 1 - rows if flip else rows
    src = grid.gather(src_rows, cols).astype(np.float32, copy=False)
    return (
        src,
        [np.searchsorted(rows, s) for s in row_sets],
        [np.searchsorted(cols, s) for s in col_sets],
    )


def resample_window(
    grid: SparseGrid,
    total_h: int,
    total_w: int,
    rows: tuple[int, int],
    cols: tuple[int, int],
    *,
    flip: bool,
    nearest: bool,
) -> np.ndarray:
    """Rows ``rows[0]:rows[1]`` and cols ``cols[0]:cols[1]`` of the slice
    resampled to (total_h, total_w), north to south, as float32.

    Where an output pixel covers several source cells, the mean of all of
    them; nearer the source's own resolution, bilinear (NaN if any of the 4
    neighbours is NaN). Categorical variables take the nearest cell either
    way, so codes are never blended. Only the source cells the result needs
    are read, so a coarse LOD stays small.
    """
    if not nearest:
        mean = aggregate_window(grid, total_h, total_w, rows, cols, flip=flip)
        if mean is not None:
            return mean

    sy = _positions(grid.n_i, total_h, *rows)
    sx = _positions(grid.n_j, total_w, *cols)

    if nearest:
        y = np.minimum((sy + 0.5).astype(np.intp), grid.n_i - 1)
        x = np.minimum((sx + 0.5).astype(np.intp), grid.n_j - 1)
        src, (ry,), (rx,) = _gather(grid, flip, [y], [x])
        return src[np.ix_(ry, rx)]

    y0 = sy.astype(np.intp)
    y1 = np.minimum(y0 + 1, grid.n_i - 1)
    x0 = sx.astype(np.intp)
    x1 = np.minimum(x0 + 1, grid.n_j - 1)
    dy = (sy - y0)[:, None]
    dx = (sx - x0)[None, :]
    src, (ry0, ry1), (rx0, rx1) = _gather(grid, flip, [y0, y1], [x0, x1])
    # NaN in any neighbour propagates through the arithmetic.
    top = src[np.ix_(ry0, rx0)] * (1.0 - dx) + src[np.ix_(ry0, rx1)] * dx
    bot = src[np.ix_(ry1, rx0)] * (1.0 - dx) + src[np.ix_(ry1, rx1)] * dx
    return (top * (1.0 - dy) + bot * dy).astype(np.float32)


def normalize_fallback(
    arr: np.ndarray, lo: float, hi: float, out_max: int
) -> np.ndarray:
    """``arr`` scaled to [0, out_max], NaN as 0 (no-numba fallback)."""
    span = hi - lo if hi != lo else 1.0
    result = np.clip((np.nan_to_num(arr, nan=0.0) - lo) / span * out_max, 0, out_max)
    return result.astype(np.uint32 if out_max > 255 else np.uint8)


def normalize(
    arr: np.ndarray, lo: float, hi: float, out_max: int
) -> tuple[np.ndarray, np.ndarray]:
    """float32 -> uint32 (or uint8 if out_max <= 255), plus a valid mask."""
    if _HAS_NUMBA:
        with _PARALLEL_KERNEL_LOCK:
            if out_max > 255:
                return _numba_normalize_uint32(arr, lo, hi, out_max)
            return _numba_normalize_uint8(arr, lo, hi, out_max)
    norm = normalize_fallback(arr, lo, hi, out_max)
    valid = (~np.isnan(arr)).astype(np.uint8)
    return norm, valid


def warmup_kernels() -> None:
    """Compile the kernels at startup, so the first tile isn't slow."""
    if _HAS_NUMBA:
        # No lock needed: nothing else runs yet.
        sample = np.zeros((32, 32), dtype=np.float32)
        _numba_normalize_uint32(sample, 0.0, 1.0, 16777215)
        _numba_normalize_uint8(sample, 0.0, 1.0, 255)
