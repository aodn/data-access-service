import logging
import math
import time
from http import HTTPStatus

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
from data_access_service.tiler.services.product.registry import (
    iter_product_items,
    iter_products,
)
from data_access_service.tiler.services.store.registry import get_available_dates
from data_access_service.tiler.utils.geo import dataset_bounds

from .shared import (
    DATE_EX,
    PRODUCT_EX,
    get_product_or_404,
    load_slice_or_404,
    validate_date,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Failed opens are not cached by StoreRegistry, so without this every /manifest
# call re-attempts every broken store at full S3 timeout — and ogcapi-java calls
# this route on every getCollectionProducts.
_STORE_FAILURE_COOLDOWN_SECONDS = 60.0
_recent_store_failures: dict[str, tuple[float, Exception]] = {}


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
        "Returns available dates for every product. "
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
    # iter_product_items returns a snapshot list so a concurrent reload can't
    # raise RuntimeError ("dictionary changed size during iteration") here.
    items = iter_product_items()
    dates_by_store = _available_dates_per_store(
        {product.source_path for _, product in items}
    )

    products = {}
    for product_id, product in items:
        all_dates = dates_by_store[product.source_path]
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


def _available_dates_per_store(store_urls: set[str]) -> dict[str, list[str]]:
    """Resolve available dates once per unique store, isolating per-store failure."""
    resolved: dict[str, list[str]] = {}
    failures: dict[str, Exception] = {}
    now = time.monotonic()

    for store_url in sorted(store_urls):
        recent = _recent_store_failures.get(store_url)
        if recent and now - recent[0] < _STORE_FAILURE_COOLDOWN_SECONDS:
            # Replay it, so the answer stays stable across the window.
            resolved[store_url] = []
            failures[store_url] = recent[1]
            continue
        try:
            resolved[store_url] = get_available_dates(store_url)
            _recent_store_failures.pop(store_url, None)
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                logger.warning(f"Availability store is absent: {store_url} ({e})")
            else:
                logger.exception(f"Availability lookup failed for store: {store_url}")
            _recent_store_failures[store_url] = (now, e)
            resolved[store_url] = []
            failures[store_url] = e

    if failures and len(failures) == len(store_urls):
        # Absent differs from unreachable: the app handler turns this into a
        # 404 naming the store.
        if all(isinstance(e, FileNotFoundError) for e in failures.values()):
            raise next(iter(failures.values()))
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="No product store could be opened; availability is unknown.",
        )
    if failures:
        logger.warning(
            "%d of %d stores failed to resolve availability; their products report "
            "empty date ranges: %s",
            len(failures),
            len(store_urls),
            ", ".join(sorted(failures)),
        )
    return resolved


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
