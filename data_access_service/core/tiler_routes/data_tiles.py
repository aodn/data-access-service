from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.openapi.models import Example

from .products import router as products_router
from .shared import DATE_EX, PRODUCT_EX

router = APIRouter()
router.include_router(products_router)


@router.get(
    "/{product_id}/{z}/{x}/{y}.png",
    summary="Raw data tile",
    description="Not implemented on the parquet-backed tiler. Use /visual_tiles.",
)
async def get_tile(
    request: Request,
    product_id: str = Path(openapi_examples=PRODUCT_EX),
    date: str = Query(openapi_examples=DATE_EX),
    z: int = Path(openapi_examples={"default": Example(value=1)}),
    x: int = Path(openapi_examples={"default": Example(value=0)}),
    y: int = Path(openapi_examples={"default": Example(value=0)}),
):
    raise HTTPException(
        status_code=501,
        detail="Data tiles are not served from the parquet vector store. Use /tiler/visual_tiles.",
    )
