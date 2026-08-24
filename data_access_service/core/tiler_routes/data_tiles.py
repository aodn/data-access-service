from fastapi import APIRouter, HTTPException, Path, Query, Response
from fastapi.openapi.models import Example

from data_access_service.config.http_cache import IMMUTABLE_CACHE_HEADERS
from data_access_service.tiler.schemas.data_tiles import DataTileManifestResponse
from data_access_service.tiler.services.product.manifest import render_manifest
from data_access_service.tiler.services.product.product import get_lod_grids
from data_access_service.tiler.services.rendering.data_tiles import render_tile

from .products import router as products_router
from .shared import (
    DATE_EX,
    PRODUCT_EX,
    get_product_or_404,
    is_store_available_or_404,
    load_slice_or_404,
    parse_date_or_422,
    resolve_timestamp_or_404,
)

router = APIRouter()
router.include_router(products_router)


@router.get(
    "/{product_id}/{z}/{x}/{y}.png",
    summary="Raw data tile",
    description=(
        "Returns an RGBA PNG encoded for WebGL shader consumption. "
        "Scalar products use R/G/B as a 24-bit normalised uint; UV vector products pack U in R and V in G. "
        "Fetch the manifest first to get the normalisation ranges needed for decoding. "
        "`date` must be one of the exact UTC timestamps returned by `/manifest`'s `available_dates`."
    ),
)
def get_tile(
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    z: int = Path(openapi_examples={"default": Example(value=1)}),
    x: int = Path(openapi_examples={"default": Example(value=0)}),
    y: int = Path(openapi_examples={"default": Example(value=0)}),
):
    product = get_product_or_404(product_id)
    is_store_available_or_404(product)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)
    lod_grids = get_lod_grids(product)

    if z not in lod_grids:
        raise HTTPException(
            status_code=404, detail=f"LOD {z} not available for {product_id}"
        )

    grid_cols, grid_rows = lod_grids[z]
    if x < 0 or x >= grid_cols or y < 0 or y >= grid_rows:
        raise HTTPException(
            status_code=404,
            detail=f"Tile {z}/{x}/{y} out of bounds (grid {grid_cols}×{grid_rows})",
        )

    variables = product.variables
    png_bytes = render_tile(
        product,
        lambda: load_slice_or_404(
            product.source_path, ts, variables, ocean_masked=product.ocean_masked
        ),
        z,
        x,
        y,
        date,
    )
    return Response(
        content=png_bytes, media_type="image/png", headers=IMMUTABLE_CACHE_HEADERS
    )


@router.get(
    "/{product_id}/manifest.json",
    summary="Data tile manifest",
    description=(
        "Returns the LOD grid dimensions and value normalisation ranges for a product on a given date. "
        "Required for decoding raw data tiles — provides `valueRange` for scalar products and `uRange`/`vRange` for UV vector products. "
        "`date` must be one of the exact UTC timestamps returned by `/manifest`'s `available_dates`."
    ),
    response_model=DataTileManifestResponse,
    response_model_exclude_none=True,
)
def get_manifest(
    response: Response,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
):
    product = get_product_or_404(product_id)
    is_store_available_or_404(product)
    ts = parse_date_or_422(date)
    resolve_timestamp_or_404(product, ts)
    get_lod_grids(product)
    variables = product.variables
    ds = load_slice_or_404(
        product.source_path, ts, variables, ocean_masked=product.ocean_masked
    )
    response.headers.update(IMMUTABLE_CACHE_HEADERS)
    return DataTileManifestResponse(**render_manifest(product, ds))
