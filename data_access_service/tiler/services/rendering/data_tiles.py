"""Data tiles: resample the slice to the LOD grid, cut out the (x, y) chunk
and encode it as PNG.

Scalars are a 24-bit value in R/G/B with the mask in alpha. Pairs put one
variable each in R and G, the mask in B, and keep alpha opaque.
"""

import math
from collections.abc import Callable

import numpy as np
import xarray as xr

from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.rendering.kernels import (
    normalize,
    resample_variables_to_grid,
)
from data_access_service.tiler.services.rendering.masks import (
    inpaint_nearest,
    land_mask_for_grid,
)
from data_access_service.tiler.utils.image import encode_rgba

# Not cached, but tiles loading together share one computation.
_processed_dedup = Deduper()


def _var_range(ds: xr.Dataset, var: str) -> tuple[float, float]:
    lo = float(ds[var].min(skipna=True).values)
    hi = float(ds[var].max(skipna=True).values)
    # All-NaN slice: use any range; every pixel is masked anyway.
    if math.isnan(lo) or math.isnan(hi):
        return (0.0, 1.0)
    return (lo, hi) if hi != lo else (lo, lo + 1.0)


def _compute_processed(
    product: Product, ds: xr.Dataset, lod: int
) -> tuple[list[np.ndarray], np.ndarray]:
    """Resample and normalise each variable on the LOD grid.

    Returns ``(normalised, ocean)``: one array per variable (24-bit for a
    scalar, 8-bit per variable for a pair), and a 0/1 mask that is 1 where
    every variable has a value.
    """
    data_tile = product.data_tile
    grid_cols, grid_rows = data_tile.lod_grids[lod]
    total_w = grid_cols * data_tile.chunk_px[0]
    total_h = grid_rows * data_tile.chunk_px[1]
    variables = product.variables

    raw = resample_variables_to_grid(ds, variables, total_w, total_h)
    # Fill toward the coast first, so filled cells count as valid.
    if data_tile.coastal_fill is not None:
        raw = [inpaint_nearest(r, data_tile.coastal_fill.max_dist_px) for r in raw]

    # 3 bytes for a scalar, 1 byte per variable for a pair.
    out_max = 16777215 if len(variables) == 1 else 255
    normalised: list[np.ndarray] = []
    valid_masks: list[np.ndarray] = []
    for r, v in zip(raw, variables, strict=True):
        lo, hi = _var_range(ds, v)
        norm, valid = normalize(r, lo, hi, out_max)
        normalised.append(norm)
        valid_masks.append(valid)

    if len(valid_masks) == 1:
        ocean = valid_masks[0]
    else:
        ocean = valid_masks[0].copy()
        for vm in valid_masks[1:]:
            ocean &= vm

    if data_tile.coastal_fill is not None:
        lon_min, lon_max = float(ds.lon.min()), float(ds.lon.max())
        lat_min, lat_max = float(ds.lat.min()), float(ds.lat.max())
        # Don't paint filled values over land.
        land = land_mask_for_grid(lon_min, lon_max, lat_min, lat_max, total_w, total_h)
        ocean = ocean & ~land

    return normalised, ocean


def _get_processed(
    product: Product, load_ds: Callable[[], xr.Dataset], lod: int, date: str
) -> tuple[list[np.ndarray], np.ndarray]:
    """The processed grid; concurrent identical requests share one compute."""
    key = (product.store, date, tuple(product.variables), lod)

    def compute() -> tuple[list[np.ndarray], np.ndarray]:
        return _compute_processed(product, load_ds(), lod)

    return _processed_dedup.dedupe(key, compute)


def _extract_chunk(
    arr: np.ndarray,
    cx: int,
    cy: int,
    total_w: int,
    total_h: int,
    chunk_px: tuple[int, int],
    padding: int,
) -> np.ndarray:
    cw, ch = chunk_px
    row_s = cy * ch
    col_s = cx * cw

    p_row_s = max(row_s - padding, 0)
    p_row_e = min(row_s + ch + padding, total_h)
    p_col_s = max(col_s - padding, 0)
    p_col_e = min(col_s + cw + padding, total_w)

    chunk = arr[p_row_s:p_row_e, p_col_s:p_col_e]

    pad_top = padding if row_s == 0 else 0
    pad_bottom = padding if row_s + ch == total_h else 0
    pad_left = padding if col_s == 0 else 0
    pad_right = padding if col_s + cw == total_w else 0

    if pad_top or pad_bottom or pad_left or pad_right:
        chunk = np.pad(
            chunk, ((pad_top, pad_bottom), (pad_left, pad_right)), mode="edge"
        )

    return chunk


def render_tile(
    product: Product,
    load_ds: Callable[[], xr.Dataset],
    lod: int,
    cx: int,
    cy: int,
    date: str,
) -> bytes:
    normalised, ocean = _get_processed(product, load_ds, lod, date)

    data_tile = product.data_tile
    grid_cols, grid_rows = data_tile.lod_grids[lod]
    total_w = grid_cols * data_tile.chunk_px[0]
    total_h = grid_rows * data_tile.chunk_px[1]

    def chunk_of(arr: np.ndarray) -> np.ndarray:
        return _extract_chunk(
            arr, cx, cy, total_w, total_h, data_tile.chunk_px, data_tile.padding
        )

    chunks = [chunk_of(arr) for arr in normalised]
    chunk_m = chunk_of(ocean)
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
