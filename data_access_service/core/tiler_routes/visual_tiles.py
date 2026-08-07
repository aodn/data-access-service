import asyncio
import functools

import anyio
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.openapi.models import Example
from fastapi.responses import Response

from data_access_service.config.config import Config
from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)
from data_access_service.tiler.schemas.visual_tiles import ColormapListResponse
from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.colormap.legend import render_legend
from data_access_service.tiler.services.colormap.registry import list_colormaps
from data_access_service.tiler.services.rendering.visual_tiles import (
    render_bbox,
    render_bbox_animation,
    render_tile,
)
from data_access_service.tiler.services.store.registry import get_available_dates
from data_access_service.tiler.services.store.slice_loader import (
    load_slice_uncached,
)
from data_access_service.tiler.services.store.spatial import (
    bbox_to_wgs84,
    default_bbox_from_store,
    native_resolution_in_bbox,
)
from data_access_service.tiler.utils.image import (
    AnimatedFormat,
    ImageFormat,
    animated_media_type,
    media_type,
)

from .products import router as products_router
from .shared import (
    DATE_EX,
    PRODUCT_EX,
    load_slice_or_404,
    parse_rescale,
    resolve_colormap_or_error,
    single_variable_or_400,
    validate_date,
    visual_product_or_400,
)

_MAX_ANIMATION_FRAMES = 30

# OGC's WebMercatorQuad well-known TileMatrixSet defines levels 0-24. Also bounds z before
# 1 << z below: negative z raises an uncaught ValueError (negative shift count), and z without
# an upper bound lets a caller force construction of an arbitrarily large Python int.
_MAX_ZOOM = 24

# Capacity gate for /animation per-frame S3 fan-out. Sits on the shared anyio
# pool as a *separate* concurrency budget from tile handlers — a 30-frame
# request cannot starve tile-handler slots. Sized to the aiobotocore S3
# connection-pool ceiling (~10/host) — going higher just queues on the pool.
_ANIMATION_LIMITER = anyio.CapacityLimiter(
    Config.get_config().get_tiler_config().animation_workers
)

router = APIRouter()
router.include_router(products_router)

_tile_dedup = Deduper()
_bbox_dedup = Deduper()


@router.get(
    "/colormaps",
    summary="List available colormaps",
    response_model=ColormapListResponse,
)
async def get_colormaps(response: Response):
    response.headers.update(REVALIDATE_CACHE_HEADERS)
    return list_colormaps()


@router.get(
    "/colormaps/{name}/legend",
    summary="Color legend",
    description=(
        "Returns a PNG color legend for the named colormap. "
        "The name must be one returned by GET /visual_tiles/colormaps. "
        "If rescale=min,max is provided, tick labels at lo, mid, and hi are drawn alongside the bar. "
        "Without rescale, only the color bar is rendered (no labels). "
        "Categorical colormaps render discrete equal-width color blocks instead of a smooth gradient."
    ),
)
def get_legend(
    name: str,
    rescale: str | None = Query(
        None,
        description=(
            "Value range as 'min,max'. When provided, tick labels are drawn at lo, mid, and hi. "
            "Rejected for categorical colormaps, whose discrete blocks have no scale to label."
        ),
    ),
    width: int = Query(256, ge=10, le=2048, description="Image width in pixels."),
    height: int = Query(40, ge=10, le=2048, description="Image height in pixels."),
    orientation: str = Query(
        "horizontal",
        description="'horizontal' (color bar left→right) or 'vertical' (color bar top→bottom).",
        pattern="^(horizontal|vertical)$",
    ),
):
    resolve_colormap_or_error(name, status_code=404)
    rescale_range = parse_rescale(rescale)
    try:
        png = render_legend(name, rescale_range, width, height, orientation)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return Response(
        content=png, media_type="image/png", headers=IMMUTABLE_CACHE_HEADERS
    )


@router.get(
    "/{product_id}/{date}/{z}/{x}/{y}.{ext}",
    summary="Visualisation raster tile",
    description=(
        "Standard Web Mercator (XYZ) tile rendered as a colourised PNG or WebP. "
        "Compatible with MapboxGL `raster` sources and any slippy-map library. "
        "Tiles outside the product extent return transparent images. "
        "WebP is rejected for categorical colormaps because lossy compression corrupts the discrete colour boundaries."
    ),
)
def get_tile(
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Path(pattern=r"^\d{4}-\d{2}-\d{2}$", openapi_examples=DATE_EX),
    z: int = Path(openapi_examples={"default": Example(value=1)}),
    x: int = Path(openapi_examples={"default": Example(value=0)}),
    y: int = Path(openapi_examples={"default": Example(value=0)}),
    ext: ImageFormat = Path(  # noqa: B008
        pattern="^(png|webp)$",
        description="Output image format — 'png' (lossless) or 'webp' (lossy, ~50% smaller).",
    ),
    colormap_name: str | None = Query(
        None,
        alias="colormap",
        description=(
            "Matplotlib or rio-tiler colormap name, e.g. viridis, plasma, RdBu_r. "
            "Omit to use the default (viridis for continuous products, the categorical "
            "palette for flag-valued products). Passing a continuous colormap to a "
            "categorical product is rejected."
        ),
    ),
    rescale: str | None = Query(
        None,
        description=(
            "Value range as 'min,max'. Defaults to the global data range for the date. "
            "Rejected for categorical products, which have no continuous scale to rescale."
        ),
    ),
):
    if colormap_name is not None:
        resolve_colormap_or_error(colormap_name)
    product = visual_product_or_400(product_id)
    validate_date(date)
    variable = single_variable_or_400(product, context="visual tiles")

    if not (0 <= z <= _MAX_ZOOM):
        raise HTTPException(
            status_code=400,
            detail=f"z={z} out of range; valid range is 0-{_MAX_ZOOM}.",
        )

    max_index = (1 << z) - 1
    if not (0 <= x <= max_index and 0 <= y <= max_index):
        raise HTTPException(
            status_code=400,
            detail=f"Tile ({x},{y}) out of range for z={z}; valid range is 0–{max_index}.",
        )

    rescale_range = parse_rescale(rescale)

    key = (
        product.source_path,
        date,
        variable,
        z,
        x,
        y,
        colormap_name,
        rescale_range,
        ext,
    )

    def _do_render() -> bytes:
        ds = load_slice_or_404(
            product.source_path, date, [variable], ocean_masked=product.ocean_masked
        )
        return render_tile(
            ds,
            variable,
            x,
            y,
            z,
            colormap_name,
            rescale_range,
            fmt=ext,
            coastal_fill=product.visual_tile.coastal_fill,
            source_path=product.source_path,
            date=date,
        )

    try:
        body = _tile_dedup.dedupe(key, _do_render)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return Response(
        content=body, media_type=media_type(ext), headers=IMMUTABLE_CACHE_HEADERS
    )


def _resolve_resolution(
    product_source_path: str,
    bbox_tuple: tuple[float, float, float, float],
    crs: str,
    width: int | None,
    height: int | None,
    max_dim: int = 2048,
) -> tuple[int, int]:
    """Fill in missing width/height per the documented defaulting rules.

    Both omitted → dataset native cell count inside the bbox.
    One provided → the other is derived from the bbox aspect ratio (in the bbox's
    own CRS), so the output frame is not stretched relative to the requested view.
    """
    if width is not None and height is not None:
        return width, height

    if width is None and height is None:
        bbox_wgs84 = bbox_to_wgs84(bbox_tuple, crs)
        return native_resolution_in_bbox(product_source_path, bbox_wgs84, max_dim)

    minx, miny, maxx, maxy = bbox_tuple
    span_x = (maxx - minx) or 1.0
    span_y = (maxy - miny) or 1.0
    aspect = span_x / span_y

    if height is None:
        # Exactly one is None at this point — narrow with a runtime check for mypy.
        assert width is not None
        derived_h = max(1, min(max_dim, round(width / aspect)))
        return width, derived_h

    assert width is None
    derived_w = max(1, min(max_dim, round(height * aspect)))
    return derived_w, height


_WEB_MERCATOR_EXTENT = (
    20_037_508.342789244  # EPSG:3857 world-square half-extent, meters
)

# Plausible geographic-degree magnitude: lon up to 360 (covers the documented
# antimeridian workaround, e.g. 57..185), lat strictly -90..90.
_DEGREE_LON_RANGE = (-180.0, 360.0)
_DEGREE_LAT_RANGE = (-90.0, 90.0)


def _looks_like_degrees(bbox: tuple[float, float, float, float]) -> bool:
    """True if every coordinate is individually plausible as a lon/lat degree value.

    This checks magnitude, not span — a degree bbox can be anywhere from a few
    meters to 360 degrees wide, so a span-based "is this too small" check can't
    catch a wide degree bbox (e.g. -180,-90,180,90, span 360x180) misread as
    Mercator metres. Checking each coordinate's own plausible range catches it
    regardless of span: a genuine Web Mercator bbox with every coordinate this
    small would describe a sub-360-metre crop sitting right at the map's origin
    (0degN, 0degE) — not a realistic request against this service's IMOS ocean products.
    """
    minx, miny, maxx, maxy = bbox
    lon_lo, lon_hi = _DEGREE_LON_RANGE
    lat_lo, lat_hi = _DEGREE_LAT_RANGE
    return (
        lon_lo <= minx <= lon_hi
        and lon_lo <= maxx <= lon_hi
        and lat_lo <= miny <= lat_hi
        and lat_lo <= maxy <= lat_hi
    )


def _validate_bbox_for_crs(bbox: tuple[float, float, float, float], crs: str) -> None:
    """Reject a bbox whose magnitudes don't plausibly match the claimed crs's units.

    Without this, swapping crs (e.g. passing degree-scale numbers with
    crs=EPSG:3857, or meter-scale numbers with crs=EPSG:4326) silently produces
    a nonsense crop instead of an error — bbox_to_wgs84 has no way to detect
    the units are wrong on its own.
    """
    minx, miny, maxx, maxy = bbox
    if crs == "EPSG:4326":
        if not (-90.0 <= miny <= 90.0 and -90.0 <= maxy <= 90.0):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"bbox latitude out of range for crs=EPSG:4326 (expected -90..90): "
                    f"miny={miny}, maxy={maxy}. Did you mean crs=EPSG:3857?"
                ),
            )
    else:  # EPSG:3857
        lo, hi = -_WEB_MERCATOR_EXTENT, _WEB_MERCATOR_EXTENT
        if not (
            lo <= minx <= hi
            and lo <= maxx <= hi
            and lo <= miny <= hi
            and lo <= maxy <= hi
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"bbox out of range for crs=EPSG:3857 (expected within "
                    f"±{hi:.0f} meters): {bbox}. Did you mean crs=EPSG:4326?"
                ),
            )
        if _looks_like_degrees(bbox):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"bbox {bbox} looks like geographic degrees, not EPSG:3857 meters "
                    "(every value falls within typical lon/lat range). "
                    "Did you mean crs=EPSG:4326?"
                ),
            )


def _parse_bbox_and_crs(
    bbox: str | None, crs: str, source_path: str
) -> tuple[tuple[float, float, float, float], str, str]:
    """Validate the crs param and parse the bbox string.

    Returns (bbox_tuple, bounds_crs, dst_crs):
    - ``bounds_crs`` is the CRS used to interpret ``bbox_tuple``'s numbers. When
      bbox is None, this is forced to EPSG:4326 regardless of the requested crs,
      because the dataset's native bounds (``default_bbox_from_store``) are
      always reported in WGS84.
    - ``dst_crs`` is the caller's requested output projection — the validated
      ``crs`` value, unaffected by whether bbox was given. It decides the CRS
      of the *rendered* image, not just how the input bbox is read.
    """
    crs = crs.upper()
    if crs not in ("EPSG:4326", "EPSG:3857"):
        raise HTTPException(
            status_code=400, detail="crs must be 'EPSG:4326' or 'EPSG:3857'"
        )
    if bbox is None:
        return default_bbox_from_store(source_path), "EPSG:4326", crs
    try:
        minx, miny, maxx, maxy = (float(v) for v in bbox.split(","))
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail="bbox must be 'minx,miny,maxx,maxy'"
        ) from e
    _validate_bbox_for_crs((minx, miny, maxx, maxy), crs)
    return (minx, miny, maxx, maxy), crs, crs


@router.get(
    "/{product_id}/{date}/bbox.{ext}",
    summary="Visualisation tile by bbox",
    description=(
        "Renders a colourised PNG or WebP for an arbitrary bounding box. The crs parameter "
        "controls both how the input bbox numbers are interpreted and the projection of the "
        "output image — same value for both, matching the OGC WMS SRS/CRS convention. "
        "Default is EPSG:3857 (Web Mercator), so with no crs argument this is a drop-in "
        "raster source for Mapbox GL / MapLibre / Leaflet / OpenLayers. Pass crs=EPSG:4326 "
        "for a Plate Carrée crop instead — e.g. for non-slippy-map / scientific consumers "
        "that want geographic-degree pixel spacing. "
        "WebP is rejected for categorical colormaps because lossy compression corrupts the discrete colour boundaries."
    ),
)
def get_bbox(
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Path(pattern=r"^\d{4}-\d{2}-\d{2}$", openapi_examples=DATE_EX),
    ext: ImageFormat = Path(  # noqa: B008
        pattern="^(png|webp)$",
        description="Output image format — 'png' (lossless) or 'webp' (lossy, ~50% smaller).",
    ),
    bbox: str | None = Query(
        None,
        description=(
            "Bounding box as 'minx,miny,maxx,maxy' in the CRS specified by the crs "
            "parameter — Web Mercator meters for 'EPSG:3857' (default), geographic degrees "
            "for 'EPSG:4326'. Defaults to the dataset's native bounds (interpreted as "
            "EPSG:4326, regardless of crs, when bbox is omitted)."
        ),
    ),
    width: int = Query(256, ge=1, le=2048),
    height: int = Query(256, ge=1, le=2048),
    colormap_name: str | None = Query(
        None,
        alias="colormap",
        description=(
            "Colormap name. Omit to use the default (viridis for continuous products, the "
            "categorical palette for flag-valued products). A continuous colormap on a "
            "categorical product is rejected."
        ),
    ),
    rescale: str | None = Query(
        None,
        description=(
            "Value range as 'min,max'. Rejected for categorical products, which have no "
            "continuous scale to rescale."
        ),
    ),
    crs: str = Query(
        "EPSG:3857",
        description=(
            "CRS of both the input bbox numbers and the rendered output image. "
            "'EPSG:3857' (default) for Web Mercator meters — matches Mapbox's "
            "{bbox-epsg-3857} placeholder and needs no client-side reprojection. "
            "'EPSG:4326' for geographic degrees in and a Plate Carrée image out. "
            "Has no effect on output when bbox is omitted (see bbox)."
        ),
    ),
):
    if colormap_name is not None:
        resolve_colormap_or_error(colormap_name)
    product = visual_product_or_400(product_id)
    validate_date(date)
    variable = single_variable_or_400(product, context="visual tiles")

    bbox_tuple, bounds_crs, dst_crs = _parse_bbox_and_crs(
        bbox, crs, product.source_path
    )

    rescale_range = parse_rescale(rescale)

    key = (
        product.source_path,
        date,
        variable,
        bbox_tuple,
        width,
        height,
        bounds_crs,
        dst_crs,
        colormap_name,
        rescale_range,
        ext,
    )

    def _do_render() -> bytes:
        ds = load_slice_or_404(
            product.source_path, date, [variable], ocean_masked=product.ocean_masked
        )
        return render_bbox(
            ds,
            variable,
            bbox_tuple,
            width,
            height,
            colormap_name,
            rescale_range,
            crs=bounds_crs,
            dst_crs=dst_crs,
            fmt=ext,
            coastal_fill=product.visual_tile.coastal_fill,
            source_path=product.source_path,
            date=date,
        )

    try:
        body = _bbox_dedup.dedupe(key, _do_render)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return Response(
        content=body, media_type=media_type(ext), headers=IMMUTABLE_CACHE_HEADERS
    )


@router.get(
    "/{product_id}/{from_date}/{to_date}/animation.{ext}",
    summary="Animated bbox over a date range",
    description=(
        f"Renders the same bbox across every available date in [from_date, to_date] "
        f"and assembles them into an animated image (GIF / APNG / animated WebP). The crs "
        f"parameter controls both the input bbox's coordinates and the output frames' "
        f"projection (same value for both); default EPSG:3857 (Web Mercator) makes this a "
        f"drop-in raster source for Mapbox GL / MapLibre / Leaflet / OpenLayers out of the box. "
        f"Intended for demos and quick visualisations — not optimised for high traffic. "
        f"At most {_MAX_ANIMATION_FRAMES} frames per request; requests beyond that are rejected. "
        f"If bbox is omitted, the dataset's native bounds are used (clamped to ±180° lon). "
        f"If width and height are both omitted, the frame matches the dataset's native cell count "
        f"inside the bbox (capped at 2048 px per axis). If only one of width/height is given, "
        f"the other is derived from the bbox aspect ratio so the output is not stretched. "
        f"This endpoint bypasses the in-memory slice cache so it never evicts hot tiles, so "
        f"expect cold requests to be slow. Like other tile endpoints the HTTP response itself "
        f"is cached for a year at the CDN since it's fully determined by the URL."
    ),
)
async def get_animation(
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    from_date: str = Path(pattern=r"^\d{4}-\d{2}-\d{2}$", openapi_examples=DATE_EX),
    to_date: str = Path(pattern=r"^\d{4}-\d{2}-\d{2}$", openapi_examples=DATE_EX),
    ext: AnimatedFormat = Path(  # noqa: B008
        pattern="^(gif|apng|webp)$",
        description="Animated output format — 'gif' (universal, 256-colour palette), 'apng' (lossless RGBA), or 'webp' (compressed RGBA).",
    ),
    bbox: str | None = Query(
        None,
        description=(
            "Bounding box as 'minx,miny,maxx,maxy' in the CRS specified by the crs "
            "parameter — Web Mercator meters for 'EPSG:3857' (default), geographic degrees "
            "for 'EPSG:4326'. Output frames are reprojected to that same crs. Defaults to "
            "the dataset's native bounds (interpreted as EPSG:4326, regardless of crs, when "
            "bbox is omitted)."
        ),
    ),
    width: int | None = Query(
        None,
        ge=1,
        le=2048,
        description=(
            "Output frame width in pixels. If both width and height are omitted, the frame matches the "
            "dataset's native cell count inside the bbox (capped at 2048). If only height is given, "
            "width is derived from the bbox aspect ratio."
        ),
    ),
    height: int | None = Query(
        None,
        ge=1,
        le=2048,
        description=(
            "Output frame height in pixels. If both width and height are omitted, the frame matches the "
            "dataset's native cell count inside the bbox (capped at 2048). If only width is given, "
            "height is derived from the bbox aspect ratio."
        ),
    ),
    colormap_name: str | None = Query(
        None,
        alias="colormap",
        description=(
            "Colormap name. Omit to use the default (viridis for continuous products, the "
            "categorical palette for flag-valued products). A continuous colormap on a "
            "categorical product is rejected."
        ),
    ),
    rescale: str | None = Query(
        None,
        description=(
            "Value range as 'min,max'. Defaults to the union range across all frames so the "
            "colour ramp stays stable. Rejected for categorical products, which have no "
            "continuous scale to rescale."
        ),
    ),
    crs: str = Query(
        "EPSG:3857",
        description=(
            "CRS of both the input bbox numbers and the rendered output frames. "
            "'EPSG:3857' (default) for Web Mercator meters — needs no client-side "
            "reprojection. 'EPSG:4326' for geographic degrees in and Plate Carrée frames "
            "out. Has no effect on output when bbox is omitted (see bbox)."
        ),
    ),
    duration: int = Query(
        200, ge=10, le=5000, description="Milliseconds per frame in the animation."
    ),
):
    validate_date(from_date)
    validate_date(to_date)
    if from_date > to_date:
        raise HTTPException(
            status_code=400,
            detail=f"from_date {from_date!r} is after to_date {to_date!r}.",
        )

    if colormap_name is not None:
        resolve_colormap_or_error(colormap_name)
    product = visual_product_or_400(product_id)
    variable = single_variable_or_400(product, context="animation")

    # Offloaded: each may call get_store, which can block on xr.open_zarr on
    # cold path or while a TTL refresh is racing the cached entry.
    bbox_tuple, bounds_crs, dst_crs = await anyio.to_thread.run_sync(
        _parse_bbox_and_crs, bbox, crs, product.source_path
    )

    rescale_range = parse_rescale(rescale)
    # Categorical validation (format, colormap↔variable fit) runs inside
    # render_bbox_animation, where the loaded slice's attrs are available; a
    # ValueError there is mapped to 400 below.

    available = await anyio.to_thread.run_sync(get_available_dates, product.source_path)
    if not available:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for product {product_id!r}.",
        )
    earliest, latest = available[0], available[-1]
    if from_date < earliest or to_date > latest:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Requested range [{from_date}, {to_date}] is outside the available dates "
                f"for product {product_id!r} ([{earliest}, {latest}])."
            ),
        )
    dates = [d for d in available if from_date <= d <= to_date]
    if not dates:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No data for product {product_id!r} between {from_date} and {to_date} "
                f"(available range: [{earliest}, {latest}])."
            ),
        )
    if len(dates) > _MAX_ANIMATION_FRAMES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Date range yields {len(dates)} frames; max is {_MAX_ANIMATION_FRAMES}. "
                "Narrow the range and retry."
            ),
        )

    resolved_w, resolved_h = await anyio.to_thread.run_sync(
        _resolve_resolution, product.source_path, bbox_tuple, bounds_crs, width, height
    )

    # Fan out the per-frame S3 reads in parallel on the anyio pool, gated by
    # _ANIMATION_LIMITER so a many-frame request does not consume tile-handler
    # slots. asyncio.gather preserves input order so frames stay in date order.
    datasets = await asyncio.gather(
        *(
            anyio.to_thread.run_sync(
                load_slice_uncached,
                product.source_path,
                d,
                [variable],
                product.ocean_masked,
                limiter=_ANIMATION_LIMITER,
            )
            for d in dates
        )
    )

    try:
        body = await anyio.to_thread.run_sync(
            functools.partial(
                render_bbox_animation,
                datasets,
                variable,
                bbox_tuple,
                resolved_w,
                resolved_h,
                colormap_name,
                rescale_range,
                crs=bounds_crs,
                dst_crs=dst_crs,
                fmt=ext,
                duration_ms=duration,
                coastal_fill=product.visual_tile.coastal_fill,
                source_path=product.source_path,
                dates=dates,
            )
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return Response(
        content=body,
        media_type=animated_media_type(ext),
        headers=IMMUTABLE_CACHE_HEADERS,
    )
