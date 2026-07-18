import hashlib
import math

import xarray as xr
from fastapi import APIRouter, Header, HTTPException, Path, Query, Response
from fastapi.openapi.models import Example
from fastapi.responses import JSONResponse

from data_access_service.config.tiler.constants import CACHE_VERSION
from data_access_service.tiler.schemas.products import (
    CoverageDiscoveryResponse,
    ManifestResponse,
    PointResponse,
    ProductConfig,
    ProductInspection,
    VariableValue,
)
from data_access_service.tiler.services.product.coverage import (
    build_coverage_discovery,
)
from data_access_service.tiler.services.product.inspect import inspect_product
from data_access_service.tiler.services.product.registry import (
    iter_product_items,
    list_products,
)
from data_access_service.tiler.services.store.registry import (
    get_available_dates,
    get_store,
)
from data_access_service.tiler.utils.geo import dataset_bounds

from .shared import (
    DATE_EX,
    IMMUTABLE_CACHE_HEADERS,
    PRODUCT_EX,
    get_product_or_404,
    load_slice_or_404,
    validate_date,
)

router = APIRouter()

# Manifest responses are revalidated via ETag (If-None-Match → 304 when unchanged), with a
# 5-minute freshness window so CloudFront can absorb concurrent reads from multiple users
# without each one round-tripping to origin. Therefore, this endpoint need to be cached in CloudFront
# with "must-revalidate" to ensure clients re-check with the origin at least every 5 minutes.
# Trade-off: a manifest change can be invisible for up to 5 minutes; acceptable because product/date
# updates are not real-time-critical.
_REVALIDATE_HEADERS = {"Cache-Control": "public, max-age=300, must-revalidate"}


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


def _etag(fingerprint: str) -> str:
    digest = hashlib.sha1(fingerprint.encode(), usedforsecurity=False).hexdigest()[:16]
    return f'W/"{digest}"'


def _etag_response(body: object, etag: str, if_none_match: str | None) -> Response:
    headers = {**_REVALIDATE_HEADERS, "ETag": etag}
    if if_none_match == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(content=body, headers=headers)


def _coverage_discovery_response(
    collection_id: str,
    from_date: str | None,
    to_date: str | None,
    if_none_match: str | None,
) -> Response:
    """Coverage discovery for one portal collection, with the same ETag/304
    revalidation contract as the availability manifest (dates arrive daily, so
    the document is mutable on the same cadence)."""
    doc = build_coverage_discovery(collection_id, from_date, to_date)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"No coverage products for collection: {collection_id}",
        )
    fingerprint_parts = [
        f"cv={CACHE_VERSION}",
        f"collection={collection_id}",
        f"from={from_date or ''}",
        f"to={to_date or ''}",
    ]
    for product in doc["products"]:
        times = product["times"]
        fingerprint_parts.append(
            f"{product['id']}:{len(times)}:{times[-1]['dateKey'] if times else ''}"
        )
    return _etag_response(doc, _etag("|".join(fingerprint_parts)), if_none_match)


@router.get(
    "/products",
    summary="List products",
    response_model=list[ProductConfig],
    response_model_exclude_none=True,
)
async def get_products():
    return [ProductConfig(**p) for p in list_products()]


@router.get(
    "/manifest",
    summary="Products availability",
    description=(
        "Returns available dates for every product. "
        "`from` defaults to each product's earliest available date; `to` is unbounded by default. "
        "With `collection_id`, returns the coverage discovery document for that portal "
        "collection instead: coverage-enabled products with grid geometry, TileMatrixSet id "
        "and datetime↔dateKey mappings (no source paths), for the OGC coverage-tile facade."
    ),
    # response_model=ManifestResponse,  # can't use this because of the dynamic ETag-based 304 response
    responses={
        200: {"model": ManifestResponse},
        304: {"description": "Not Modified — ETag matched, response body is empty"},
        404: {
            "description": "collection_id given but no coverage-enabled product is associated with it"
        },
    },
)
def get_products_availability(
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
    collection_id: str | None = Query(
        None,
        description=(
            "Portal collection UUID. Switches the response to the coverage discovery "
            "document (see CoverageDiscoveryResponse) filtered to that collection."
        ),
    ),
    if_none_match: str | None = Header(None, alias="if-none-match"),
    # Automatically sent by browser using previous ETag from previous response.
):
    if collection_id is not None:
        return _coverage_discovery_response(
            collection_id, from_date, to_date, if_none_match
        )

    products = {}

    fingerprint_parts = [
        f"cv={CACHE_VERSION}",
        f"from={from_date or ''}",
        f"to={to_date or ''}",
    ]
    # iter_product_items returns a snapshot list so a concurrent reload can't
    # raise RuntimeError ("dictionary changed size during iteration") here.
    for product_id, product in iter_product_items():
        all_dates = get_available_dates(product.source_path)
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
        fingerprint_parts.append(
            f"{product_id}:{len(dates)}:{dates[-1] if dates else ''}"
        )

    etag = _etag("|".join(fingerprint_parts))
    return _etag_response(
        {"products": products, "cache_version": CACHE_VERSION}, etag, if_none_match
    )


@router.get(
    "/{product_id}/inspect",
    summary="Inspect product",
    description=(
        "Returns the product's underlying Zarr store metadata: dimension sizes, and "
        "per-variable dtype, shape, native chunk shape, and attributes — plus the "
        "dataset's global attributes. Useful for debugging and client introspection."
    ),
    response_model=ProductInspection,
)
def inspect(
    response: Response,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
):
    product = get_product_or_404(product_id)
    try:
        ds = get_store(product.source_path)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    # Revalidate (not immutable): the store grows as new dates land, so dimension
    # sizes change over time. Mirror /manifest's freshness window — see
    # _REVALIDATE_HEADERS — rather than freezing the first response forever.
    response.headers.update(_REVALIDATE_HEADERS)
    return ProductInspection(**inspect_product(product, ds))


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
