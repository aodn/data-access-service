"""Visual tiles: reproject a slice with rio-tiler, apply a colormap and
encode PNG or WebP.

Grids that cross 180°E are split in two, since rio-tiler requires lon within
±180; the parts are composited before encoding.
"""

import logging
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import xarray as xr
from rio_tiler.colormap import apply_cmap
from rio_tiler.errors import TileOutsideBounds
from rio_tiler.io.xarray import XarrayReader
from rio_tiler.models import ImageData
from rioxarray.exceptions import NoDataInBounds

from data_access_service.tiler.services.caching.deduper import Deduper
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
from data_access_service.tiler.services.store.spatial import bbox_to_wgs84
from data_access_service.tiler.utils.image import (
    AnimatedFormat,
    ImageFormat,
    empty_tile,
    encode_rgba,
    encode_rgba_animation,
)

logger = logging.getLogger(__name__)

# Tiles loading together share one coastal fill.
_fill_dedup = Deduper()

# ...and one _to_scalar_parts.
_scalar_parts_dedup = Deduper()


def _get_filled_values(
    store: str,
    date: str,
    variable: str,
    coastal_fill: CoastalFill,
    values: np.ndarray,
    lons: np.ndarray,
    lats: np.ndarray,
) -> np.ndarray:
    """``values`` with the coastal fill applied and land cut out. Shared
    between callers, so read-only."""
    key = (store, date, variable, coastal_fill.max_dist_px)

    def compute() -> np.ndarray:
        filled = inpaint_nearest(values, coastal_fill.max_dist_px).copy()
        land = land_mask_for_coords(lons, lats)
        filled[land] = np.nan
        filled.setflags(write=False)
        return filled

    return _fill_dedup.dedupe(key, compute)


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


def _to_scalar_parts(
    ds: xr.Dataset,
    variable: str,
    coastal_fill: CoastalFill | None = None,
    store: str = "",
    date: str = "",
) -> list[xr.DataArray]:
    """The variable as float32 DataArrays for rio-tiler: one, or two when the
    grid crosses 180°E (the part east of 180 shifted by -360).

    Applies ``coastal_fill`` first, if set. Shared between callers per
    (store, date, variable, coastal_fill), so read-only.
    """
    key = (
        store,
        date,
        variable,
        coastal_fill.max_dist_px if coastal_fill is not None else None,
    )

    def compute() -> list[xr.DataArray]:
        da = ds[variable].astype(np.float32)
        if coastal_fill is not None:
            filled = _get_filled_values(
                store,
                date,
                variable,
                coastal_fill,
                da.values,
                da.lon.values,
                da.lat.values,
            )
            da = da.copy(data=filled)

        lat_min, lat_max = float(da.lat.min()), float(da.lat.max())
        lon_min, lon_max = float(da.lon.min()), float(da.lon.max())
        if not (
            -90 <= lat_min and lat_max <= 90 and -180 <= lon_min and lon_max <= 360
        ):
            raise ValueError(
                f"Dataset '{variable}' does not appear to be in EPSG:4326: "
                f"lat [{lat_min:.1f}, {lat_max:.1f}], lon [{lon_min:.1f}, {lon_max:.1f}]. "
                "Expected lat ∈ [−90, 90] and lon ∈ [−180, 360]."
            )

        if float(da.lon.max()) > 180:
            normalised = np.where(
                da.lon.values > 180, da.lon.values - 360, da.lon.values
            )
            native_res = abs(float(da.lon.values[1] - da.lon.values[0]))
            max_gap = float(np.max(np.diff(np.sort(normalised))))

            if max_gap <= 2 * native_res:
                # Contiguous after wrapping: a global grid.
                da = da.assign_coords(lon=("lon", normalised)).sortby("lon")
                parts = [_apply_crs(da)]
            else:
                # Crosses 180°E: split in two, leaving out lon=180 exactly.
                primary = _apply_crs(da.sel(lon=da.lon[da.lon < 180]))
                minor_da = da.sel(lon=da.lon[da.lon > 180])
                minor_da = _apply_crs(
                    minor_da.assign_coords(
                        lon=("lon", minor_da.lon.values - 360)
                    ).sortby("lon")
                )
                parts = [primary, minor_da]
        else:
            parts = [_apply_crs(da)]

        for part in parts:
            part.values.setflags(write=False)
        return parts

    return _scalar_parts_dedup.dedupe(key, compute)


_rescale_dedup = Deduper()


def _rescale_range(
    parts: list[xr.DataArray],
    rescale: tuple[float, float] | None,
    *,
    store: str = "",
    date: str = "",
    variable: str = "",
    coastal_fill: CoastalFill | None = None,
) -> tuple[float, float] | None:
    """``rescale``, else the data range; None if no data."""
    if rescale is not None:
        return rescale

    def compute() -> tuple[float, float] | None:
        all_valid = np.concatenate(
            [p.values[~np.isnan(p.values)].ravel() for p in parts]
        )
        if not all_valid.size:
            return None
        return float(all_valid.min()), float(all_valid.max())

    if not date:
        return compute()

    key = (
        store,
        date,
        variable,
        coastal_fill.max_dist_px if coastal_fill is not None else None,
    )
    return _rescale_dedup.dedupe(key, compute)


def render_tile(
    ds: xr.Dataset,
    variable: str,
    x: int,
    y: int,
    z: int,
    colormap_name: str | None = None,
    rescale: tuple[float, float] | None = None,
    fmt: ImageFormat = "png",
    coastal_fill: CoastalFill | None = None,
    store: str = "",
    date: str = "",
) -> bytes:
    """A 256x256 Web Mercator tile; transparent outside the data."""
    attrs = ds[variable].attrs
    _validate_categorical_request(variable, attrs, colormap_name, fmt, rescale=rescale)
    parts = _to_scalar_parts(ds, variable, coastal_fill, store, date)

    if is_categorical_variable(attrs):
        scheme = resolve_scheme(attrs, colormap_name)
        result = _categorical_composite(
            parts, scheme.lut(), lambda r: r.tile(x, y, z, reproject_method="nearest")
        )
        return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)

    vrange = _rescale_range(
        parts,
        rescale,
        store=store,
        date=date,
        variable=variable,
        coastal_fill=coastal_fill,
    )
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
    ds: xr.Dataset,
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
    store: str = "",
    date: str = "",
) -> bytes:
    """An image of ``bbox`` (in ``crs``), rendered in ``dst_crs``;
    transparent if it misses the data."""
    attrs = ds[variable].attrs
    _validate_categorical_request(variable, attrs, colormap_name, fmt, rescale=rescale)
    parts = _to_scalar_parts(ds, variable, coastal_fill, store, date)
    bbox_wgs84 = bbox_to_wgs84(bbox, crs)
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

    vrange = _rescale_range(
        parts,
        rescale,
        store=store,
        date=date,
        variable=variable,
        coastal_fill=coastal_fill,
    )
    if vrange is None:
        return empty_tile(fmt)
    vmin, vmax = vrange
    span = vmax - vmin or 1.0
    cm = resolve_colormap(colormap_name or "viridis")

    result = _bbox_parts_to_rgba(
        parts, bbox_wgs84, width, height, vmin, span, cm, dst_crs=dst_crs
    )
    return encode_rgba(result, fmt) if result is not None else empty_tile(fmt)


def render_bbox_animation(
    datasets: list[xr.Dataset],
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
    coastal_fill: CoastalFill | None = None,
    store: str = "",
    dates: list[str] | None = None,
) -> bytes:
    """The same bbox for each dataset, as an animated image.

    Without ``rescale``, one range across all frames, so colours don't
    flicker. Frames outside the data are transparent.
    """
    if not datasets:
        raise ValueError("render_bbox_animation requires at least one dataset")
    if dates is None:
        dates = [""] * len(datasets)

    attrs = datasets[0][variable].attrs
    _validate_categorical_request(
        variable, attrs, colormap_name, fmt, rescale=rescale, animated=True
    )

    parts_per_frame = [
        _to_scalar_parts(ds, variable, coastal_fill, store, d)
        for ds, d in zip(datasets, dates, strict=True)
    ]
    bbox_wgs84 = bbox_to_wgs84(bbox, crs)

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

    if rescale is not None:
        vmin, vmax = rescale
    else:
        all_parts = [p for parts in parts_per_frame for p in parts]
        vrange = _rescale_range(all_parts, None)
        if vrange is None:
            empty = np.zeros((height, width, 4), dtype=np.uint8)
            return encode_rgba_animation([empty] * len(datasets), fmt, duration_ms)
        vmin, vmax = vrange

    span = vmax - vmin or 1.0
    cm = resolve_colormap(colormap_name or "viridis")

    frames: list[np.ndarray] = []
    for parts in parts_per_frame:
        rgba = _bbox_parts_to_rgba(
            parts, bbox_wgs84, width, height, vmin, span, cm, dst_crs=dst_crs
        )
        if rgba is None:
            rgba = np.zeros((height, width, 4), dtype=np.uint8)
        frames.append(rgba)

    return encode_rgba_animation(frames, fmt, duration_ms)
