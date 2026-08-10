import math
from typing import Callable, Literal

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
    is_store_available_or_404,
    get_product_or_404,
    load_slice_or_404,
    validate_date,
)

ProductFilter = Callable[[Product], bool]


def _require_point_in_bounds(ds: xr.Dataset, lat: float, lon: float) -> None:
    """Raise 404 if (lat, lon) falls outside the dataset's coverage.

    sel(method="nearest") snaps unconditionally, so without this guard an
    out-of-bounds request silently returns the edge cell. Bounds match those
    advertised by /manifest.
    """
    lon_min, lon_max, lat_min, lat_max = dataset_bounds(ds)
    if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
        raise HTTPException(
            status_code=404,
            detail=(
                f"Point ({lat}, {lon}) is outside the data bounds "
                f"(lat {lat_min}..{lat_max}, lon {lon_min}..{lon_max})"
            ),
        )


StoreStatus = Literal["all", "available", "unavailable"]


def build_products_router(*, product_filter: ProductFilter | None = None) -> APIRouter:
    """Build the /products, /manifest, /{product_id}/{date}/point router.

    Mounted separately by data_tiles.py (product_filter=None) and
    visual_tiles.py (product_filter=is_single_variable) — a multi-variable
    product (e.g. the UCUR+VCUR currents product) 400s on every visual_tiles
    single-product endpoint (see single_variable_or_400), so listing it there
    would advertise something that never actually renders. /point isn't
    restricted this way (it already reports every variable, single or not),
    so it's identical at both mounts.
    """
    router = APIRouter()

    @router.get(
        "/products",
        summary="List products",
        response_model=list[ProductConfig],
    )
    async def get_products(
        response: Response,
        store_status: StoreStatus = Query(
            "available",
            description=(
                "Filter by whether the product's store available. "
                "Default 'available' only; 'unavailable' or 'all' to see the rest."
            ),
        ),
    ):
        products = iter_products()
        if product_filter is not None:
            products = [p for p in products if product_filter(p)]
        if store_status == "available":
            products = [p for p in products if is_store_available(p.source_path)]
        elif store_status == "unavailable":
            products = [p for p in products if not is_store_available(p.source_path)]
        response.headers.update(REVALIDATE_CACHE_HEADERS)
        return [ProductConfig.from_product(p) for p in products]

    @router.get(
        "/manifest",
        summary="Products availability",
        description=(
            "Returns available dates for every available product. "
            "`from` defaults to each product's earliest available date; `to` is unbounded by default."
        ),
        response_model=ManifestResponse,
    )
    def get_products_availability(
        response: Response,
        from_date: str | None = Query(
            None,
            alias="from",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description="Start date (inclusive), YYYY-MM-DD. Defaults to each product's earliest available date.",
            openapi_examples={"default": Example(value="2024-01-01")},
        ),
        to_date: str | None = Query(
            None,
            alias="to",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description="End date (inclusive), YYYY-MM-DD. Defaults to no upper bound.",
            openapi_examples={"default": Example(value="2024-12-31")},
        ),
    ):
        products = {}

        # iter_product_items returns a snapshot list so a concurrent reload can't
        # raise RuntimeError ("dictionary changed size during iteration") here.
        for product_id, product in iter_product_items():
            if product_filter is not None and not product_filter(product):
                continue
            all_dates = get_available_dates(product.source_path)
            if not all_dates:
                continue
            # full_date_range is the product's full dataset bounds, independent of from/to;
            # available_dates below is the from/to-filtered subset.
            dates = all_dates
            if from_date:
                dates = [d for d in dates if d >= from_date]
            if to_date:
                dates = [d for d in dates if d <= to_date]
            products[product_id] = {
                "available_dates": dates,
                "full_date_range": {
                    "start": all_dates[0] if all_dates else None,
                    "end": all_dates[-1] if all_dates else None,
                },
            }

        response.headers.update(REVALIDATE_CACHE_HEADERS)
        return {"products": products}

    @router.get(
        "/{product_id}/{date}/point",
        summary="Point value lookup",
        description="Returns the value(s) of all product variables at the nearest grid cell to the given lat/lon.",
        response_model=PointResponse,
    )
    def get_point(
        response: Response,
        product_id: str = Path(openapi_examples=PRODUCT_EX),
        date: str = Path(pattern=r"^\d{4}-\d{2}-\d{2}$", openapi_examples=DATE_EX),
        lat: float = Query(..., openapi_examples={"default": Example(value=-33.8)}),
        lon: float = Query(..., openapi_examples={"default": Example(value=151.2)}),
    ):
        product = get_product_or_404(product_id)
        is_store_available_or_404(product)
        validate_date(date)
        variables = product.variables
        ds = load_slice_or_404(
            product.source_path, date, variables, ocean_masked=product.ocean_masked
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

        response.headers.update(IMMUTABLE_CACHE_HEADERS)
        return PointResponse(
            lat=float(point.lat.values),
            lon=float(point.lon.values),
            variables=values,
        )

    return router
