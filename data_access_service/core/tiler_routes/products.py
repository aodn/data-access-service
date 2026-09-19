import functools
import logging
import math

import anyio
import pandas as pd
import xarray as xr
from fastapi import APIRouter, HTTPException, Path, Query, Response
from fastapi.openapi.models import Example

from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)
from data_access_service.tiler.schemas.products import (
    ManifestResponse,
    PointResponse,
    ProductConfig,
    VariableValue,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import (
    iter_product_items,
    iter_products,
)
from data_access_service.tiler.services.store.registry import (
    get_available_dates,
    is_store_available,
)
from data_access_service.tiler.utils.geo import dataset_bounds

from .shared import (
    DATE_EX,
    PRODUCT_EX,
    TILE_THREAD_LIMITER,
    get_product_or_404,
    is_store_available_or_404,
    load_slice_or_404,
    parse_date_or_422,
    resolve_timestamp_or_404,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _require_point_in_bounds(ds: xr.Dataset, lat: float, lon: float) -> None:
    """404 if (lat, lon) is outside the data, instead of snapping to the edge."""
    lon_min, lon_max, lat_min, lat_max = dataset_bounds(ds)
    if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
        raise HTTPException(
            status_code=404,
            detail=(
                f"Point ({lat}, {lon}) is outside the data bounds "
                f"(lat {lat_min}..{lat_max}, lon {lon_min}..{lon_max})"
            ),
        )


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
    description=(
        "Returns available dates for every product, or only products belonging to "
        "a given metadata_uuid. `from` and `to` are unbounded by default."
    ),
    response_model=ManifestResponse,
)
async def get_products_availability(
    response: Response,
    from_date: str | None = Query(
        None,
        alias="from",
        description=(
            "Start instant (inclusive), full UTC ISO-8601 timestamp. "
            "Defaults to no lower bound."
        ),
        openapi_examples={"default": Example(value="2024-01-01T00:00:00Z")},
    ),
    to_date: str | None = Query(
        None,
        alias="to",
        description=(
            "End instant (inclusive), full UTC ISO-8601 timestamp. "
            "Defaults to no upper bound."
        ),
        openapi_examples={"default": Example(value="2024-12-31T00:00:00Z")},
    ),
    metadata_uuid: str | None = Query(
        None,
        description=(
            "Restrict results to products linked to this GeoNetwork/STAC collection "
            "UUID. Defaults to every registered product. 404s if no product matches."
        ),
    ),
):
    from_ts = parse_date_or_422(from_date) if from_date else None
    to_ts = parse_date_or_422(to_date) if to_date else None

    items = iter_product_items()
    if metadata_uuid is not None:
        items = [
            (product_id, product)
            for product_id, product in items
            if product.metadata_uuid == metadata_uuid
        ]
        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"No products found for metadata_uuid {metadata_uuid!r}.",
            )

    products = {}
    for product_id, product in items:
        if not is_store_available(product.store):
            continue

        all_dates = get_available_dates(product.store)
        if not all_dates:
            continue
        # full_date_range ignores from/to; available_dates doesn't.
        dates = [
            d
            for d, ts in all_dates
            if (from_ts is None or ts >= from_ts) and (to_ts is None or ts <= to_ts)
        ]
        products[product_id] = {
            "available_dates": dates,
            "full_date_range": {
                "start": all_dates[0][0] if all_dates else None,
                "end": all_dates[-1][0] if all_dates else None,
            },
        }

    response.headers.update(REVALIDATE_CACHE_HEADERS)
    return {"products": products}


@router.get(
    "/{product_id}/point",
    summary="Point value lookup",
    description=(
        "Returns the value(s) of all product variables at the nearest grid cell to the given lat/lon. "
        "`date` must be one of the exact UTC timestamps returned by `/manifest`'s `available_dates`."
    ),
    response_model=PointResponse,
)
async def get_point(
    response: Response,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    lat: float = Query(..., openapi_examples={"default": Example(value=-33.8)}),
    lon: float = Query(..., openapi_examples={"default": Example(value=151.2)}),
):
    product = get_product_or_404(product_id)
    is_store_available_or_404(product)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)

    result = await anyio.to_thread.run_sync(
        functools.partial(_load_point, product, ts, lat, lon),
        limiter=TILE_THREAD_LIMITER,
    )

    response.headers.update(IMMUTABLE_CACHE_HEADERS)
    return result


def _load_point(
    product: Product, ts: pd.Timestamp, lat: float, lon: float
) -> PointResponse:
    variables = product.variables
    ds = load_slice_or_404(
        product.store, ts, variables, ocean_masked=product.ocean_masked
    )

    _require_point_in_bounds(ds, lat, lon)
    point = ds.sel(lat=lat, lon=lon, method="nearest")

    values: dict[str, VariableValue] = {}
    for var in variables:
        v = float(point[var].squeeze())
        values[var] = VariableValue(
            value=None if math.isnan(v) else v,
            units=point[var].attrs.get("units"),
        )

    return PointResponse(
        lat=float(point.lat.values),
        lon=float(point.lon.values),
        variables=values,
    )
