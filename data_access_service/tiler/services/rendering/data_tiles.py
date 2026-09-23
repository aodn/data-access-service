"""Data tiles: resample one (x, y) chunk of the LOD grid from the slice and
encode it as PNG. Only the chunk (plus padding) is computed.

Scalars are a 24-bit value in R/G/B with the mask in alpha. Pairs put one
variable each in R and G, the mask in B, and keep alpha opaque.
"""

import math
from collections.abc import Callable

import numpy as np

from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.rendering.kernels import (
    normalize,
    resample_window,
)
from data_access_service.tiler.services.rendering.masks import (
    inpaint_nearest,
    land_mask_for_grid,
)
from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    SparseSlice,
)
from data_access_service.tiler.utils.image import encode_rgba

EMPTY_RANGE = (0.0, 1.0)


def _var_range(grid: SparseGrid) -> tuple[float, float]:
    lo, hi = grid.vmin, grid.vmax
    if math.isnan(lo) or math.isnan(hi):
        return EMPTY_RANGE
    return (lo, hi) if hi != lo else (lo, lo + 1.0)


def _chunk_window(
    cx: int,
    cy: int,
    total_w: int,
    total_h: int,
    chunk_px: tuple[int, int],
    padding: int,
) -> tuple[tuple[int, int], tuple[int, int], tuple[tuple[int, int], ...]]:
    """The chunk's rows and cols on the LOD grid, with padding but clipped
    to the grid, and the edge padding still to add at the grid border."""
    cw, ch = chunk_px
    row_s = cy * ch
    col_s = cx * cw
    rows = (max(row_s - padding, 0), min(row_s + ch + padding, total_h))
    cols = (max(col_s - padding, 0), min(col_s + cw + padding, total_w))
    pads = (
        (padding if row_s == 0 else 0, padding if row_s + ch == total_h else 0),
        (padding if col_s == 0 else 0, padding if col_s + cw == total_w else 0),
    )
    return rows, cols, pads


def _compute_window(
    product: Product,
    sparse: SparseSlice,
    lod: int,
    rows: tuple[int, int],
    cols: tuple[int, int],
) -> tuple[list[np.ndarray], np.ndarray]:
    """Resample and normalise each variable on this window of the LOD grid.

    Returns ``(normalised, ocean)``: one array per variable (24-bit for a
    scalar, 8-bit per variable for a pair), and a 0/1 mask that is 1 where
    every variable has a value.
    """
    data_tile = product.data_tile
    grid_cols, grid_rows = data_tile.lod_grids[lod]
    total_w = grid_cols * data_tile.chunk_px[0]
    total_h = grid_rows * data_tile.chunk_px[1]
    variables = product.variables
    fill = data_tile.coastal_fill

    # The coastal fill looks up to max_dist_px away, so resample that much more.
    halo = fill.max_dist_px if fill is not None else 0
    ext_rows = (max(rows[0] - halo, 0), min(rows[1] + halo, total_h))
    ext_cols = (max(cols[0] - halo, 0), min(cols[1] + halo, total_w))
    crop = (
        slice(rows[0] - ext_rows[0], rows[1] - ext_rows[0]),
        slice(cols[0] - ext_cols[0], cols[1] - ext_cols[0]),
    )

    # Resample north to south.
    flip = float(sparse.lat[0]) < float(sparse.lat[-1])
    raw = []
    for v in variables:
        r = resample_window(
            sparse.grids[v],
            total_h,
            total_w,
            ext_rows,
            ext_cols,
            flip=flip,
            nearest=is_categorical_variable(sparse.attrs[v]),
        )
        # Fill toward the coast first, so filled cells count as valid.
        if fill is not None:
            r = inpaint_nearest(r, fill.max_dist_px)
        raw.append(r[crop])

    # 3 bytes for a scalar, 1 byte per variable for a pair.
    out_max = 16777215 if len(variables) == 1 else 255
    normalised: list[np.ndarray] = []
    valid_masks: list[np.ndarray] = []
    for r, v in zip(raw, variables, strict=True):
        lo, hi = _var_range(sparse.grids[v])
        norm, valid = normalize(r, lo, hi, out_max)
        normalised.append(norm)
        valid_masks.append(valid)

    ocean = valid_masks[0].copy()
    for vm in valid_masks[1:]:
        ocean &= vm

    if fill is not None:
        lon_min, lon_max, lat_min, lat_max = sparse.bounds()
        # Don't paint filled values over land.
        land = land_mask_for_grid(
            lon_min,
            lon_max,
            lat_min,
            lat_max,
            total_w,
            total_h,
            rows=slice(*rows),
            cols=slice(*cols),
        )
        ocean = ocean & ~land

    return normalised, ocean


def render_tile(
    product: Product,
    load_slice: Callable[[], SparseSlice],
    lod: int,
    cx: int,
    cy: int,
) -> bytes:
    data_tile = product.data_tile
    grid_cols, grid_rows = data_tile.lod_grids[lod]
    total_w = grid_cols * data_tile.chunk_px[0]
    total_h = grid_rows * data_tile.chunk_px[1]
    rows, cols, pads = _chunk_window(
        cx, cy, total_w, total_h, data_tile.chunk_px, data_tile.padding
    )
    normalised, ocean = _compute_window(product, load_slice(), lod, rows, cols)

    def pad(arr: np.ndarray) -> np.ndarray:
        if any(p for pair in pads for p in pair):
            return np.pad(arr, pads, mode="edge")
        return arr

    chunks = [pad(arr) for arr in normalised]
    chunk_m = pad(ocean)
    h, w = chunk_m.shape
    img = np.zeros((h, w, 4), dtype=np.uint8)

    if len(chunks) == 1:
        # Scalar: 24-bit value in R/G/B, mask in alpha; RGB zeroed off the mask.
        val = chunks[0]
        img[:, :, 0] = (val >> 16) & 0xFF
        img[:, :, 1] = (val >> 8) & 0xFF
        img[:, :, 2] = val & 0xFF
        img[:, :, 3] = chunk_m * 255
        img[chunk_m == 0, :3] = 0
    else:
        # Pair: variables in R and G, mask in B, alpha opaque.
        img[:, :, 0] = chunks[0]
        img[:, :, 1] = chunks[1]
        img[:, :, 2] = chunk_m * 255
        img[:, :, 3] = 255

    return encode_rgba(img)
