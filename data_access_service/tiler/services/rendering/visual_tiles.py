"""Visual tiles: reproject a slice with rio-tiler, apply a colormap and
encode PNG or WebP.

Only the part of the grid under the tile or bbox (plus a small margin) is
built. Grids that cross 180°E are split in two, since rio-tiler requires lon
within ±180; the parts are composited before encoding.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import morecantile
import numpy as np
import xarray as xr
from rio_tiler.colormap import apply_cmap
from rio_tiler.errors import TileOutsideBounds
from rio_tiler.io.xarray import XarrayReader
from rio_tiler.models import ImageData
from rioxarray.exceptions import NoDataInBounds

from data_access_service.tiler.services.colormap.categorical import (
    RGBA,
    is_categorical_variable,
    resolve_scheme,
)
from data_access_service.tiler.services.colormap.registry import (
    get_category_values,
    is_categorical,
)
from data_access_service.tiler.services.colormap.resolver import resolve_colormap
from data_access_service.tiler.services.product.product import CoastalFill
from data_access_service.tiler.services.rendering.masks import (
    inpaint_nearest,
    land_mask_for_coords,
)
from data_access_service.tiler.services.store.sparse_grid import SparseGrid, SparseSlice
from data_access_service.tiler.services.store.spatial import bbox_to_wgs84
from data_access_service.tiler.utils.image import (
    AnimatedFormat,
    ImageFormat,
    empty_tile,
    encode_rgba,
    encode_rgba_animation,
)

logger = logging.getLogger(__name__)

_WEB_MERCATOR = morecantile.tms.get("WebMercatorQuad")

# Extra source pixels around the window, so rio-tiler sees the neighbours it
# would have seen in the whole grid.
_MARGIN_PX = 3

# Aggregate once an output pixel covers more than this many source cells.
# Below it, read the cells; above it, reading them all to interpolate between
# four is wasted.
_OVERSAMPLE = 4

# Blocks per output pixel when aggregating. Two, not one, so rio-tiler still
# has neighbours to interpolate between.
_AVG_OVERSAMPLE = 2

# The most cells one part reads. A 256 tile stays under it; a large bbox or
# animation frame would otherwise read the whole grid cell for cell.
_MAX_READ_CELLS = 2048 * 2048


def warmup_visual() -> None:
    """Warm up rio-tiler and GDAL at startup, so the first tile isn't slow."""
    da = xr.DataArray(
        np.zeros((16, 16), dtype=np.float32),
        dims=("lat", "lon"),
        coords={"lat": np.linspace(1.0, 0.0, 16), "lon": np.linspace(0.0, 1.0, 16)},
    )
    da = _apply_crs(da)
    # A plain grey LUT, so this doesn't need the colormap registry.
    cm = {i: (i, i, i, 255) for i in range(256)}
    try:
        with XarrayReader(da) as reader:
            img = reader.tile(0, 0, 0, reproject_method="bilinear")
        img.rescale(in_range=[(0.0, 1.0)])
        _img_to_rgba(img, cm)
    except Exception:
        logger.exception("Visual warmup failed")


def _img_to_rgba(
    img: ImageData, cm: dict[int, tuple[int, int, int, int]]
) -> np.ndarray:
    """A rescaled image -> (H, W, 4) RGBA, before encoding."""
    rgb, cmap_alpha = apply_cmap(img.data, cm)  # rgb: (3, H, W), cmap_alpha: (H, W)
    rgba = np.empty((rgb.shape[1], rgb.shape[2], 4), dtype=np.uint8)
    rgba[..., 0] = rgb[0]
    rgba[..., 1] = rgb[1]
    rgba[..., 2] = rgb[2]
    # Transparent if the colormap or the data mask says so.
    rgba[..., 3] = np.minimum(cmap_alpha, img.mask.astype(np.uint8))
    return rgba


def _img_to_rgba_categorical(img: ImageData, lut: dict[int, RGBA]) -> np.ndarray:
    """A categorical image -> (H, W, 4) RGBA; each code indexes the LUT."""
    codes = np.nan_to_num(img.data, nan=0.0).astype(np.uint8)  # (1, H, W)
    rgb, cmap_alpha = apply_cmap(codes, lut)
    rgba = np.empty((rgb.shape[1], rgb.shape[2], 4), dtype=np.uint8)
    rgba[..., 0] = rgb[0]
    rgba[..., 1] = rgb[1]
    rgba[..., 2] = rgb[2]
    # Opaque only where there is data and the colour is opaque.
    valid = (~np.isnan(img.data[0])).astype(np.uint8) * 255
    rgba[..., 3] = np.minimum(cmap_alpha, valid)
    return rgba


def _composite_over(base: np.ndarray, top: np.ndarray) -> np.ndarray:
    """Copy ``top``'s opaque pixels onto ``base``."""
    mask = top[..., 3] > 0
    base[mask] = top[mask]
    return base


def _categorical_composite(
    parts: list[xr.DataArray],
    lut: dict[int, RGBA],
    read: Callable[[XarrayReader], ImageData],
) -> np.ndarray | None:
    """Read, colour and composite each part; None if none intersect."""
    result: np.ndarray | None = None
    for da in parts:
        try:
            with XarrayReader(da) as reader:
                img = read(reader)
        except (TileOutsideBounds, NoDataInBounds):
            continue
        rgba = _img_to_rgba_categorical(img, lut)
        result = rgba if result is None else _composite_over(result, rgba)
    return result


def _validate_categorical_request(
    variable: str,
    attrs: Mapping[str, Any],
    colormap_name: str | None,
    fmt: str,
    *,
    rescale: tuple[float, float] | None = None,
    animated: bool = False,
) -> None:
    """Reject invalid categorical requests (ValueError -> 400):

    - categorical variable with WebP, ``rescale``, a continuous colormap, or
      a categorical colormap whose values differ from flag_values;
    - continuous variable with a categorical colormap.
    """
    colormap_is_categorical = bool(colormap_name) and is_categorical(colormap_name)

    if not is_categorical_variable(attrs):
        if colormap_is_categorical:
            raise ValueError(
                f"Categorical colormap '{colormap_name}' can only be applied to a categorical "
                f"variable (one with CF flag_values); variable '{variable}' is continuous."
            )
        return

    if rescale is not None:
        raise ValueError(
            f"Variable '{variable}' is categorical; rescale does not apply (categories are "
            f"discrete codes, not a continuous scale). Omit rescale."
        )

    if fmt == "webp":
        kind = "animated WebP" if animated else "WebP"
        alternatives = "Use .apng or .gif." if animated else "Use .png."
        raise ValueError(
            f"Variable '{variable}' is categorical and cannot be encoded as {kind} "
            f"(lossy compression corrupts the discrete category boundaries). {alternatives}"
        )

    if colormap_name is not None and not colormap_is_categorical:
        raise ValueError(
            f"Variable '{variable}' is categorical; colormap '{colormap_name}' is a continuous "
            f"colormap. Pass a categorical colormap, or omit it to use the default palette."
        )

    if colormap_is_categorical:
        assert colormap_name is not None  # narrowed by colormap_is_categorical
        expected = sorted(int(v) for v in attrs["flag_values"])
        cmap_values = get_category_values(colormap_name)
        if cmap_values != expected:
            raise ValueError(
                f"Categorical colormap '{colormap_name}' covers values {cmap_values}, which do "
                f"not match variable '{variable}' flag_values {expected}."
            )


def _apply_crs(da: xr.DataArray) -> xr.DataArray:
    return da.rio.write_crs("EPSG:4326", inplace=True).rio.set_spatial_dims(
        x_dim="lon", y_dim="lat", inplace=True
    )


@dataclass(frozen=True)
class _Part:
    """Grid columns that are contiguous in lon within ±180."""

    cols: np.ndarray
    lon: np.ndarray


def _split_parts(lat: np.ndarray, lon: np.ndarray, variable: str) -> list[_Part]:
    """One part, or two when the grid crosses 180°E (the part east of 180
    shifted by -360)."""
    lat_min, lat_max = float(lat.min()), float(lat.max())
    lon_min, lon_max = float(lon.min()), float(lon.max())
    if not (-90 <= lat_min and lat_max <= 90 and -180 <= lon_min and lon_max <= 360):
        raise ValueError(
            f"Dataset '{variable}' does not appear to be in EPSG:4326: "
            f"lat [{lat_min:.1f}, {lat_max:.1f}], lon [{lon_min:.1f}, {lon_max:.1f}]. "
            "Expected lat ∈ [−90, 90] and lon ∈ [−180, 360]."
        )

    if lon_max <= 180:
        return [_Part(np.arange(len(lon)), lon)]

    normalised = np.where(lon > 180, lon - 360, lon)
    native_res = abs(float(lon[1] - lon[0]))
    max_gap = float(np.max(np.diff(np.sort(normalised))))
    if max_gap <= 2 * native_res:
        # Contiguous after wrapping: a global grid.
        order = np.argsort(normalised, kind="stable")
        return [_Part(order, normalised[order])]

    # Crosses 180°E: split in two, leaving out lon=180 exactly.
    primary = np.nonzero(lon < 180)[0]
    minor = np.nonzero(lon > 180)[0]
    minor = minor[np.argsort(lon[minor], kind="stable")]
    return [_Part(primary, lon[primary]), _Part(minor, lon[minor] - 360)]


def _window(coords: np.ndarray, lo: float, hi: float, pad: float) -> slice | None:
    """The positions of ``coords`` (monotonic) within [lo - pad, hi + pad]."""
    inside = np.nonzero((coords >= lo - pad) & (coords <= hi + pad))[0]
    if not inside.size:
        return None
    return slice(int(inside[0]), int(inside[-1]) + 1)


def _steps(
    n_rows: int, n_cols: int, out_height: int, out_width: int
) -> tuple[int, int]:
    """How many source pixels an output pixel covers, per axis, capped at one.

    ``(1, 1)`` means the window is already near the output's own resolution,
    so it is read cell for cell and the tile comes out exactly as it always
    did. Anything more means there is something to aggregate.

    Past ``_MAX_READ_CELLS``, the steps grow to about one cell per output
    pixel.
    """
    row_step = max(1, n_rows // max(1, out_height * _OVERSAMPLE))
    col_step = max(1, n_cols // max(1, out_width * _OVERSAMPLE))
    if (n_rows // row_step) * (n_cols // col_step) > _MAX_READ_CELLS:
        row_step = max(row_step, -(-n_rows // max(1, out_height)))
        col_step = max(col_step, -(-n_cols // max(1, out_width)))
    return row_step, col_step


def _is_run(idx: np.ndarray) -> bool:
    """Whether these source columns are one increasing run, so they name a
    slice. A grid whose columns had to be reordered to wrap past 180 doesn't."""
    return bool(idx.size) and bool(
        np.array_equal(idx, np.arange(idx[0], idx[0] + idx.size))
    )


def _centres(coords: np.ndarray, edges: np.ndarray, offset: int) -> np.ndarray:
    """The middle coordinate of each block the edges mark out."""
    at = edges[:-1] - offset
    return np.add.reduceat(coords, at) / np.diff(edges)


def _block_count(n: int, out: int, step: int) -> int:
    """Blocks along one axis of an aggregated window."""
    return min(n, out * _AVG_OVERSAMPLE, max(out, n // step))


def _aggregate_window(
    grid: SparseGrid,
    lat: np.ndarray,
    part_lon: np.ndarray,
    rows: slice,
    cols: slice,
    src_cols: np.ndarray,
    out_height: int,
    out_width: int,
    row_step: int,
    col_step: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """The window as block means at ``_AVG_OVERSAMPLE`` blocks per output pixel.

    Where ``_steps`` capped the read, only as many blocks as the steps leave
    cells, but never fewer than the output has pixels (or the window cells).

    Averages every cell rather than sampling one per block, and never builds
    the window at the grid's own resolution.
    """
    src = slice(int(src_cols[0]), int(src_cols[-1]) + 1)
    values, row_edges, col_edges = grid.aggregate(
        rows,
        src,
        _block_count(rows.stop - rows.start, out_height, row_step),
        _block_count(src.stop - src.start, out_width, col_step),
    )
    lat_coords = _centres(lat[rows], row_edges, rows.start)
    lon_coords = _centres(part_lon[cols], col_edges, src.start)
    return values, lat_coords, lon_coords


def _parts_in_bbox(
    sparse: SparseSlice,
    variable: str,
    bbox_wgs84: tuple[float, float, float, float],
    coastal_fill: CoastalFill | None = None,
    out_width: int = 256,
    out_height: int = 256,
) -> list[xr.DataArray]:
    """The variable under ``bbox_wgs84`` as float32 DataArrays for rio-tiler,
    one per part the bbox touches.

    A window whose cells outnumber the ``out_width`` x ``out_height`` output
    by more than ``_OVERSAMPLE`` on an axis comes back as block means, so a
    low-zoom tile never materialises the grid at its own resolution. Windows
    near the output's resolution are read cell for cell, unchanged.

    Applies ``coastal_fill`` (and cuts filled values off land), reading that
    many pixels more so the fill sees the same neighbours.
    """
    lat, lon = sparse.lat, sparse.lon
    grid = sparse.grids[variable]
    attrs = sparse.attrs[variable]
    # Averaging category codes would invent categories, so those stay sampled.
    categorical = is_categorical_variable(attrs)
    w, s, e, n = bbox_wgs84
    margin = _MARGIN_PX + (coastal_fill.max_dist_px if coastal_fill else 0)
    lat_res = abs(float(lat[1] - lat[0])) if len(lat) > 1 else 0.0
    lon_res = abs(float(lon[1] - lon[0])) if len(lon) > 1 else 0.0

    split = _split_parts(lat, lon, variable)
    plain_rows = _window(lat, s, n, 0.0)
    if plain_rows is None:
        return []

    parts = []
    for part in split:
        plain_cols = _window(part.lon, w, e, 0.0)
        if plain_cols is None:
            continue
        # The steps come from the unpadded window; the margin is then widened
        # by them so those extra pixels still land in the result rather than
        # disappearing inside a block.
        row_step, col_step = _steps(
            plain_rows.stop - plain_rows.start,
            plain_cols.stop - plain_cols.start,
            out_height,
            out_width,
        )
        rows = _window(lat, s, n, margin * row_step * lat_res)
        cols = _window(part.lon, w, e, margin * col_step * lon_res)
        if rows is None or cols is None:
            continue

        src_cols = part.cols[cols]
        # Category codes can't be averaged, and a reordered column axis can't
        # be sliced, so either one falls back to sampling every n-th cell.
        if (row_step > 1 or col_step > 1) and not categorical and _is_run(src_cols):
            values, lat_coords, lon_coords = _aggregate_window(
                grid,
                lat,
                part.lon,
                rows,
                cols,
                src_cols,
                out_height,
                out_width,
                row_step,
                col_step,
            )
        else:
            lat_coords = lat[rows][::row_step]
            lon_coords = part.lon[cols][::col_step]
            values = grid.gather(
                np.arange(len(lat))[rows][::row_step], src_cols[::col_step]
            )
            values = values.astype(np.float32, copy=False)
        if coastal_fill is not None:
            # max_dist_px counts source pixels, so it shrinks with the window.
            reach = max(1, round(coastal_fill.max_dist_px / max(row_step, col_step)))
            values = inpaint_nearest(values, reach).copy()
            values[land_mask_for_coords(lon_coords, lat_coords)] = np.nan
        da = xr.DataArray(
            values,
            dims=("lat", "lon"),
            coords={"lat": lat_coords, "lon": lon_coords},
            attrs=dict(attrs),
        )
        parts.append(_apply_crs(da))
    return parts


def _data_range(ranges: list[tuple[float, float]]) -> tuple[float, float] | None:
    """The overall min/max of these ``(vmin, vmax)`` pairs; None if no data."""
    lows = [lo for lo, _ in ranges]
    highs = [hi for _, hi in ranges]
    if all(np.isnan(lows)):
        return None
    return float(np.nanmin(lows)), float(np.nanmax(highs))


def _grid_range(sparse: SparseSlice, variable: str) -> tuple[float, float]:
    grid = sparse.grids[variable]
    return grid.vmin, grid.vmax


def _tile_bbox(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """The tile's (west, south, east, north) in lon/lat."""
    b = _WEB_MERCATOR.bounds(x, y, z)
    return (b.left, b.bottom, b.right, b.top)


def render_tile(
    sparse: SparseSlice,
    variable: str,
    x: int,
    y: int,
    z: int,
    colormap_name: str | None = None,
    rescale: tuple[float, float] | None = None,
    fmt: ImageFormat = "png",
    coastal_fill: CoastalFill | None = None,
) -> bytes:
    """A 256x256 Web Mercator tile; transparent outside the data."""
    attrs = sparse.attrs[variable]
    _validate_categorical_request(variable, attrs, colormap_name, fmt, rescale=rescale)
    parts = _parts_in_bbox(sparse, variable, _tile_bbox(x, y, z), coastal_fill)

    if is_categorical_variable(attrs):
        scheme = resolve_scheme(attrs, colormap_name)
        result = _categorical_composite(
            parts, scheme.lut(), lambda r: r.tile(x, y, z, reproject_method="nearest")
        )
        return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)

    vrange = rescale or _data_range([_grid_range(sparse, variable)])
    if vrange is None:
        return empty_tile(fmt)
    vmin, vmax = vrange
    span = vmax - vmin or 1.0
    cm = resolve_colormap(colormap_name or "viridis")
    result: np.ndarray | None = None

    for da in parts:
        try:
            with XarrayReader(da) as reader:
                img = reader.tile(x, y, z, reproject_method="bilinear")
        except TileOutsideBounds:
            continue
        img.rescale(in_range=[(vmin, vmin + span)])
        rgba = _img_to_rgba(img, cm)
        if result is None:
            result = rgba
        else:
            mask = rgba[..., 3] > 0
            result[mask] = rgba[mask]

    return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)


def _bbox_parts_to_rgba(
    parts: list[xr.DataArray],
    bbox_wgs84: tuple[float, float, float, float],
    width: int,
    height: int,
    vmin: float,
    span: float,
    cm: dict[int, tuple[int, int, int, int]],
    dst_crs: str = "EPSG:3857",
) -> np.ndarray | None:
    """Render each part into the bbox and composite; None if none intersect."""
    lon_min, lat_min, lon_max, lat_max = bbox_wgs84
    result: np.ndarray | None = None
    for da in parts:
        try:
            with XarrayReader(da) as reader:
                img = reader.part(
                    (lon_min, lat_min, lon_max, lat_max),
                    dst_crs=dst_crs,
                    width=width,
                    height=height,
                    reproject_method="bilinear",
                )
        except (TileOutsideBounds, NoDataInBounds):
            continue
        img.rescale(in_range=[(vmin, vmin + span)])
        rgba = _img_to_rgba(img, cm)
        if result is None:
            result = rgba
        else:
            mask = rgba[..., 3] > 0
            result[mask] = rgba[mask]
    return result


def render_bbox(
    sparse: SparseSlice,
    variable: str,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    colormap_name: str | None = None,
    rescale: tuple[float, float] | None = None,
    crs: str = "EPSG:4326",
    dst_crs: str = "EPSG:3857",
    fmt: ImageFormat = "png",
    coastal_fill: CoastalFill | None = None,
) -> bytes:
    """An image of ``bbox`` (in ``crs``), rendered in ``dst_crs``;
    transparent if it misses the data."""
    attrs = sparse.attrs[variable]
    _validate_categorical_request(variable, attrs, colormap_name, fmt, rescale=rescale)
    bbox_wgs84 = bbox_to_wgs84(bbox, crs)
    parts = _parts_in_bbox(sparse, variable, bbox_wgs84, coastal_fill, width, height)
    lo, la_min, hi, la_max = bbox_wgs84

    if is_categorical_variable(attrs):
        scheme = resolve_scheme(attrs, colormap_name)
        result = _categorical_composite(
            parts,
            scheme.lut(),
            lambda r: r.part(
                (lo, la_min, hi, la_max),
                dst_crs=dst_crs,
                width=width,
                height=height,
                reproject_method="nearest",
            ),
        )
        return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)

    vrange = rescale or _data_range([_grid_range(sparse, variable)])
    if vrange is None:
        return empty_tile(fmt)
    vmin, vmax = vrange
    span = vmax - vmin or 1.0
    cm = resolve_colormap(colormap_name or "viridis")

    result = _bbox_parts_to_rgba(
        parts, bbox_wgs84, width, height, vmin, span, cm, dst_crs=dst_crs
    )
    return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)


@dataclass(frozen=True)
class BboxFrame:
    """One animation frame: the slice cut to the bbox, and the whole slice's
    min/max (for one colour range across frames)."""

    parts: list[xr.DataArray]
    vmin: float
    vmax: float
    attrs: dict


def cut_frame(
    sparse: SparseSlice,
    variable: str,
    bbox: tuple[float, float, float, float],
    crs: str = "EPSG:4326",
    coastal_fill: CoastalFill | None = None,
    out_width: int = 256,
    out_height: int = 256,
) -> BboxFrame:
    """Keep only what an animation frame needs from ``sparse``.

    ``out_width``/``out_height`` are the animation's size, so each frame is
    read at the resolution it will be rendered at.
    """
    parts = _parts_in_bbox(
        sparse, variable, bbox_to_wgs84(bbox, crs), coastal_fill, out_width, out_height
    )
    vmin, vmax = _grid_range(sparse, variable)
    return BboxFrame(parts, vmin, vmax, sparse.attrs[variable])


def render_bbox_animation(
    frames: list[BboxFrame],
    variable: str,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    colormap_name: str | None = None,
    rescale: tuple[float, float] | None = None,
    crs: str = "EPSG:4326",
    dst_crs: str = "EPSG:3857",
    fmt: AnimatedFormat = "webp",
    duration_ms: int = 200,
) -> bytes:
    """The same bbox for each frame (from ``cut_frame``), as an animated image.

    Without ``rescale``, one range across all frames, so colours don't
    flicker. Frames outside the data are transparent.
    """
    if not frames:
        raise ValueError("render_bbox_animation requires at least one frame")

    attrs = frames[0].attrs
    _validate_categorical_request(
        variable, attrs, colormap_name, fmt, rescale=rescale, animated=True
    )

    bbox_wgs84 = bbox_to_wgs84(bbox, crs)
    parts_per_frame = [frame.parts for frame in frames]

    if is_categorical_variable(attrs):
        lut = resolve_scheme(attrs, colormap_name).lut()
        lo, la_min, hi, la_max = bbox_wgs84

        def _read_part(r: XarrayReader) -> ImageData:
            return r.part(
                (lo, la_min, hi, la_max),
                dst_crs=dst_crs,
                width=width,
                height=height,
                reproject_method="nearest",
            )

        cat_frames: list[np.ndarray] = []
        for parts in parts_per_frame:
            rgba = _categorical_composite(parts, lut, _read_part)
            if rgba is None:
                rgba = np.zeros((height, width, 4), dtype=np.uint8)
            cat_frames.append(rgba)
        return encode_rgba_animation(cat_frames, fmt, duration_ms)

    vrange = rescale or _data_range([(f.vmin, f.vmax) for f in frames])
    if vrange is None:
        empty = np.zeros((height, width, 4), dtype=np.uint8)
        return encode_rgba_animation([empty] * len(frames), fmt, duration_ms)
    vmin, vmax = vrange

    span = vmax - vmin or 1.0
    cm = resolve_colormap(colormap_name or "viridis")

    images: list[np.ndarray] = []
    for parts in parts_per_frame:
        rgba = _bbox_parts_to_rgba(
            parts, bbox_wgs84, width, height, vmin, span, cm, dst_crs=dst_crs
        )
        if rgba is None:
            rgba = np.zeros((height, width, 4), dtype=np.uint8)
        images.append(rgba)

    return encode_rgba_animation(images, fmt, duration_ms)
