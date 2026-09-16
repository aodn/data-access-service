"""Paint parquet cells with a LUT and encode PNG/WebP."""

from __future__ import annotations

import numpy as np
from PIL import Image

from data_access_service.tiler.catalog import get_client
from data_access_service.tiler.colormap import lut_from_name
from data_access_service.tiler.product import Product
from data_access_service.tiler.utils.dates import compact_timestamp
from data_access_service.tiler.utils.geo import bbox_to_ij_window, xyz_tile_wgs84_bbox
from data_access_service.tiler.utils.image import (
    TILE_SIZE,
    ImageFormat,
    empty_tile,
    encode_rgba,
    resize_rgba,
)


def _paint(
    i: np.ndarray,
    j: np.ndarray,
    values: np.ndarray,
    height: int,
    width: int,
    vmin: float,
    vmax: float,
    lut: np.ndarray,
) -> np.ndarray:
    grid = np.full((height, width), np.nan, dtype=np.float32)
    i = np.asarray(i, dtype=np.intp)
    j = np.asarray(j, dtype=np.intp)
    in_bounds = (i >= 0) & (i < height) & (j >= 0) & (j < width)
    grid[i[in_bounds], j[in_bounds]] = np.asarray(values)[in_bounds]
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    finite = np.isfinite(grid)
    span = vmax - vmin if vmax != vmin else 1.0
    idx = np.zeros(grid.shape, dtype=np.uint8)
    idx[finite] = np.clip((grid[finite] - vmin) / span * 255.0, 0, 255).astype(np.uint8)
    rgba[finite] = lut[idx[finite]]
    return rgba


def render_window(
    product: Product,
    date: str,
    *,
    i_min: int,
    i_max: int,
    j_min: int,
    j_max: int,
    colormap: str | None,
    vmin: float | None,
    vmax: float | None,
    width: int,
    height: int,
    fmt: ImageFormat,
) -> bytes:
    ts = compact_timestamp(date)
    cells = get_client().read_cells(
        product.uuid,
        ts,
        i_min=i_min,
        i_max=i_max,
        j_min=j_min,
        j_max=j_max,
        variable=product.variable,
        source_path=product.source_path,
    )
    h = i_max - i_min + 1
    w = j_max - j_min + 1
    if cells.empty:
        rgba = np.zeros((h, w, 4), dtype=np.uint8)
    else:
        i = cells["i"].to_numpy() - i_min
        j = cells["j"].to_numpy() - j_min
        values = cells["value"].to_numpy(dtype=np.float32)
        lo = product.vmin if vmin is None else vmin
        hi = product.vmax if vmax is None else vmax
        rgba = _paint(i, j, values, h, w, lo, hi, lut_from_name(colormap))
    if rgba.shape[0] != height or rgba.shape[1] != width:
        rgba = resize_rgba(rgba, width, height)
    return encode_rgba(rgba, fmt)


def render_tile(
    product: Product,
    date: str,
    x: int,
    y: int,
    z: int,
    colormap: str | None,
    rescale: tuple[float, float] | None,
    fmt: ImageFormat,
) -> bytes:
    lon_min, lat_min, lon_max, lat_max = xyz_tile_wgs84_bbox(x, y, z)
    window = bbox_to_ij_window(
        lon_min,
        lat_min,
        lon_max,
        lat_max,
        grid_lat_min=product.lat_min,
        grid_lat_max=product.lat_max,
        grid_lon_min=product.lon_min,
        grid_lon_max=product.lon_max,
        n_i=product.n_i,
        n_j=product.n_j,
    )
    if window is None:
        return empty_tile(fmt)
    i0, i1, j0, j1 = window
    vmin = vmax = None
    if rescale is not None:
        vmin, vmax = rescale
    return render_window(
        product,
        date,
        i_min=i0,
        i_max=i1,
        j_min=j0,
        j_max=j1,
        colormap=colormap,
        vmin=vmin,
        vmax=vmax,
        width=TILE_SIZE,
        height=TILE_SIZE,
        fmt=fmt,
    )


def render_bbox(
    product: Product,
    date: str,
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    width: int,
    height: int,
    colormap: str | None,
    rescale: tuple[float, float] | None,
    fmt: ImageFormat,
) -> bytes:
    window = bbox_to_ij_window(
        lon_min,
        lat_min,
        lon_max,
        lat_max,
        grid_lat_min=product.lat_min,
        grid_lat_max=product.lat_max,
        grid_lon_min=product.lon_min,
        grid_lon_max=product.lon_max,
        n_i=product.n_i,
        n_j=product.n_j,
    )
    if window is None:
        return encode_rgba(np.zeros((height, width, 4), dtype=np.uint8), fmt)
    i0, i1, j0, j1 = window
    vmin = vmax = None
    if rescale is not None:
        vmin, vmax = rescale
    return render_window(
        product,
        date,
        i_min=i0,
        i_max=i1,
        j_min=j0,
        j_max=j1,
        colormap=colormap,
        vmin=vmin,
        vmax=vmax,
        width=width,
        height=height,
        fmt=fmt,
    )


def render_legend(
    name: str,
    rescale: tuple[float, float] | None,
    width: int,
    height: int,
    orientation: str,
) -> bytes:
    lut = lut_from_name(name)
    if orientation == "vertical":
        bar = np.repeat(lut[::-1, None, :], width, axis=1)
        bar = np.asarray(
            Image.fromarray(bar, "RGBA").resize(
                (width, height), Image.Resampling.NEAREST
            )
        )
    else:
        bar = np.repeat(lut[None, :, :], height, axis=0)
        bar = np.asarray(
            Image.fromarray(bar, "RGBA").resize(
                (width, height), Image.Resampling.NEAREST
            )
        )
    return encode_rgba(bar, "png")


def lookup_point(product: Product, date: str, lat: float, lon: float) -> float | None:
    from data_access_service.tiler.utils.geo import latlon_to_ij

    i, j = latlon_to_ij(
        lat,
        lon,
        lat_min=product.lat_min,
        lat_max=product.lat_max,
        lon_min=product.lon_min,
        lon_max=product.lon_max,
        n_i=product.n_i,
        n_j=product.n_j,
    )
    cells = get_client().read_cells(
        product.uuid,
        compact_timestamp(date),
        i_min=i,
        i_max=i,
        j_min=j,
        j_max=j,
        variable=product.variable,
        source_path=product.source_path,
    )
    if cells.empty:
        return None
    return float(cells["value"].iloc[0])
