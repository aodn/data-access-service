import functools

from fastapi import APIRouter, HTTPException, Path, Query, Request, Response
from fastapi.openapi.models import Example

from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)
from data_access_service.tiler.colormap import list_colormaps, resolve_colormap_or_error
from data_access_service.tiler.render import render_bbox, render_legend, render_tile
from data_access_service.tiler.schemas.visual_tiles import ColormapListResponse
from data_access_service.tiler.utils.geo import bbox_to_wgs84
from data_access_service.tiler.utils.image import ImageFormat, media_type

from .products import router as products_router
from .shared import (
    DATE_EX,
    PRODUCT_EX,
    TILE_THREAD_LIMITER,
    ClientDisconnected,
    parse_date_or_422,
    parse_rescale,
    resolve_timestamp_or_404,
    run_cancellable,
    visual_product_or_400,
)

_MAX_ZOOM = 24

router = APIRouter()
router.include_router(products_router)


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
)
async def get_legend(
    name: str,
    rescale: str | None = Query(None),
    width: int = Query(256, ge=10, le=2048),
    height: int = Query(40, ge=10, le=2048),
    orientation: str = Query("horizontal", pattern="^(horizontal|vertical)$"),
):
    resolve_colormap_or_error(name, status_code=404)
    rescale_range = parse_rescale(rescale)
    png = await run_cancellable_sync(
        functools.partial(
            render_legend, name, rescale_range, width, height, orientation
        )
    )
    return Response(
        content=png, media_type="image/png", headers=IMMUTABLE_CACHE_HEADERS
    )


async def run_cancellable_sync(fn):
    import anyio

    return await anyio.to_thread.run_sync(fn, limiter=TILE_THREAD_LIMITER)


@router.get(
    "/{product_id}/{z}/{x}/{y}.{ext}",
    summary="Visualisation raster tile",
)
async def get_tile(
    request: Request,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    z: int = Path(openapi_examples={"default": Example(value=1)}),
    x: int = Path(openapi_examples={"default": Example(value=0)}),
    y: int = Path(openapi_examples={"default": Example(value=0)}),
    ext: ImageFormat = Path(pattern="^(png|webp)$"),  # noqa: B008
    colormap_name: str | None = Query(None, alias="colormap"),
    rescale: str | None = Query(None),
):
    if colormap_name is not None:
        resolve_colormap_or_error(colormap_name)
    product = visual_product_or_400(product_id)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)

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

    def _do_render() -> bytes:
        return render_tile(product, date, x, y, z, colormap_name, rescale_range, ext)

    try:
        body = await run_cancellable(request, _do_render)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ClientDisconnected as e:
        raise HTTPException(status_code=499, detail="Client disconnected") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tile render failed: {e}") from e

    return Response(
        content=body, media_type=media_type(ext), headers=IMMUTABLE_CACHE_HEADERS
    )


@router.get(
    "/{product_id}/bbox.{ext}",
    summary="Visualisation tile by bbox",
)
async def get_bbox(
    request: Request,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    ext: ImageFormat = Path(pattern="^(png|webp)$"),  # noqa: B008
    bbox: str | None = Query(None),
    width: int = Query(256, ge=1, le=2048),
    height: int = Query(256, ge=1, le=2048),
    colormap_name: str | None = Query(None, alias="colormap"),
    rescale: str | None = Query(None),
    crs: str = Query("EPSG:3857"),
):
    if colormap_name is not None:
        resolve_colormap_or_error(colormap_name)
    product = visual_product_or_400(product_id)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)
    crs = crs.upper()
    if crs not in ("EPSG:4326", "EPSG:3857"):
        raise HTTPException(
            status_code=400, detail="crs must be 'EPSG:4326' or 'EPSG:3857'"
        )
    if bbox is None:
        lon_min, lat_min, lon_max, lat_max = (
            product.lon_min,
            product.lat_min,
            product.lon_max,
            product.lat_max,
        )
    else:
        try:
            minx, miny, maxx, maxy = (float(v) for v in bbox.split(","))
        except ValueError as e:
            raise HTTPException(
                status_code=400, detail="bbox must be 'minx,miny,maxx,maxy'"
            ) from e
        lon_min, lat_min, lon_max, lat_max = bbox_to_wgs84(
            (minx, miny, maxx, maxy), crs
        )

    rescale_range = parse_rescale(rescale)

    def _do_render() -> bytes:
        return render_bbox(
            product,
            date,
            lon_min,
            lat_min,
            lon_max,
            lat_max,
            width,
            height,
            colormap_name,
            rescale_range,
            ext,
        )

    try:
        body = await run_cancellable(request, _do_render)
    except ClientDisconnected as e:
        raise HTTPException(status_code=499, detail="Client disconnected") from e

    return Response(
        content=body, media_type=media_type(ext), headers=IMMUTABLE_CACHE_HEADERS
    )
