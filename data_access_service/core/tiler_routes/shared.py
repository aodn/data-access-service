"""Helpers shared across the three routers (products, data_tiles, visual_tiles)."""

from datetime import date as _Date
from http import HTTPStatus

from fastapi import HTTPException
from fastapi.openapi.models import Example

from data_access_service.tiler.services.colormap.resolver import resolve_colormap
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import get_product
from data_access_service.tiler.services.store.slice_loader import load_slice

PRODUCT_EX: dict[str, Example] = {"default": Example(value="sea_level_anomaly")}
DATE_EX: dict[str, Example] = {"default": Example(value="2024-02-24")}


_tiler_ready = False


def mark_tiler_ready() -> None:
    global _tiler_ready
    _tiler_ready = True


def require_tiler_ready() -> None:
    """FastAPI dependency: 503 until tiler startup has finished.

    Mirrors api_instance.get_api_status() on the main data routes — without
    it, a request arriving before startup completes would just see an empty
    product/colormap registry instead of a clear "not ready" response.
    """
    if not _tiler_ready:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Tiler is not ready. Product/store initialization is still in progress.",
        )


def get_product_or_404(product_id: str) -> Product:
    product = get_product(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Unknown product: {product_id}")
    return product


def validate_date(date: str) -> None:
    try:
        _Date.fromisoformat(date)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Invalid date: {date!r}") from e


def load_slice_or_404(
    store_url: str, date: str, variables: list[str], ocean_masked: bool = False
):
    try:
        return load_slice(store_url, date, variables, ocean_masked)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


def resolve_colormap_or_error(name: str, *, status_code: int = 400) -> None:
    """Validate a colormap name, raising HTTPException on failure.

    Defaults to 400 (colormap usually arrives as a query param, so an unknown
    name is a malformed request). Callers exposing it as a path segment pass
    status_code=404 — the URL points at a resource that does not exist.
    """
    try:
        resolve_colormap(name)
    except ValueError as e:
        raise HTTPException(status_code=status_code, detail=str(e)) from e


def single_variable_or_400(product: Product, *, context: str) -> str:
    """Narrow product.variable to a single str, rejecting multi-variable products."""
    if isinstance(product.variable, list):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Product '{product.id}' has multiple variables; "
                f"{context} supports single-variable products only."
            ),
        )
    return product.variable


def parse_rescale(rescale: str | None) -> tuple[float, float] | None:
    if not rescale:
        return None
    try:
        lo, hi = rescale.split(",")
        return (float(lo), float(hi))
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail="rescale must be 'min,max', e.g. '-0.5,0.5'"
        ) from e
