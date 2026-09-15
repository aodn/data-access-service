import functools

import anyio
from fastapi import APIRouter, HTTPException, Path, Query, Response
from fastapi.openapi.models import Example

from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)
from data_access_service.tiler.catalog import available_dates_iso
from data_access_service.tiler.product import iter_product_items, iter_products
from data_access_service.tiler.render import lookup_point
from data_access_service.tiler.schemas.products import (
    ManifestResponse,
    PointResponse,
    ProductConfig,
    VariableValue,
)

from .shared import (
    DATE_EX,
    PRODUCT_EX,
    TILE_THREAD_LIMITER,
    get_product_or_404,
    parse_date_or_422,
    resolve_timestamp_or_404,
)

router = APIRouter()


@router.get(
    "/products",
    summary="List products",
    response_model=list[ProductConfig],
)
async def get_products(response: Response):
    response.headers.update(REVALIDATE_CACHE_HEADERS)
    return [ProductConfig.from_product(p) for p in iter_products()]


@router.get(
    "/manifest",
    summary="Products availability",
    response_model=ManifestResponse,
)
async def get_products_availability(
    response: Response,
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    metadata_uuid: str | None = Query(None),
):
    from_ts = parse_date_or_422(from_date) if from_date else None
    to_ts = parse_date_or_422(to_date) if to_date else None

    items = iter_product_items()
    if metadata_uuid is not None:
        items = [
            (pid, product)
            for pid, product in items
            if product.metadata_uuid == metadata_uuid
        ]
        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"No products found for metadata_uuid {metadata_uuid!r}.",
            )

    products = {}
    for product_id, product in items:
        all_iso = available_dates_iso(product)
        if not all_iso:
            continue
        dates = all_iso
        if from_ts is not None or to_ts is not None:
            filtered = []
            for d in all_iso:
                ts = parse_date_or_422(d)
                if from_ts is not None and ts < from_ts:
                    continue
                if to_ts is not None and ts > to_ts:
                    continue
                filtered.append(d)
            dates = filtered
        products[product_id] = {
            "available_dates": dates,
            "full_date_range": {
                "start": all_iso[0] if all_iso else None,
                "end": all_iso[-1] if all_iso else None,
            },
        }

    response.headers.update(REVALIDATE_CACHE_HEADERS)
    return {"products": products}


@router.get(
    "/{product_id}/point",
    summary="Point value lookup",
    response_model=PointResponse,
)
async def get_point(
    response: Response,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    lat: float = Query(...),
    lon: float = Query(...),
):
    product = get_product_or_404(product_id)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)
    value = await anyio.to_thread.run_sync(
        functools.partial(lookup_point, product, date, lat, lon),
        limiter=TILE_THREAD_LIMITER,
    )
    response.headers.update(IMMUTABLE_CACHE_HEADERS)
    return PointResponse(
        lat=lat,
        lon=lon,
        date=date,
        values=[VariableValue(variable=product.variable, value=value)],
    )
