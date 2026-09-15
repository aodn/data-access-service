"""Helpers shared across products, data_tiles, and visual_tiles routers."""

from collections.abc import Callable
from http import HTTPStatus
from typing import TypeVar

import anyio
import pandas as pd
from fastapi import HTTPException, Request
from fastapi.openapi.models import Example

from data_access_service.config.config import Config
from data_access_service.tiler.colormap import resolve_colormap_or_error
from data_access_service.tiler.product import Product, get_product
from data_access_service.tiler.utils.dates import (
    compact_timestamp,
    str_to_utc_timestamp,
)

PRODUCT_EX: dict[str, Example] = {"default": Example(value="sea_level_anomaly")}
DATE_EX: dict[str, Example] = {"default": Example(value="2024-02-24T00:00:00Z")}

T = TypeVar("T")

TILE_THREAD_LIMITER = anyio.CapacityLimiter(
    Config.get_config().get_tiler_config().thread_pool_size
)

_DISCONNECT_POLL_INTERVAL = 0.1


class ClientDisconnected(Exception):
    """Raised by run_cancellable when the client disconnects before fn completes."""


async def run_cancellable(request: Request, fn: Callable[[], T]) -> T:
    outcome: list[T] = []

    async def _runner(tg: anyio.abc.TaskGroup) -> None:
        outcome.append(
            await anyio.to_thread.run_sync(
                fn, abandon_on_cancel=True, limiter=TILE_THREAD_LIMITER
            )
        )
        tg.cancel_scope.cancel()

    async def _watch_disconnect(tg: anyio.abc.TaskGroup) -> None:
        while not await request.is_disconnected():
            await anyio.sleep(_DISCONNECT_POLL_INTERVAL)
        tg.cancel_scope.cancel()

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_runner, tg)
            tg.start_soon(_watch_disconnect, tg)
    except* Exception as eg:
        raise eg.exceptions[0] from None

    if not outcome:
        raise ClientDisconnected()
    return outcome[0]


_tiler_ready = False


def mark_tiler_ready() -> None:
    global _tiler_ready
    _tiler_ready = True


def require_tiler_ready() -> None:
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


def visual_product_or_400(product_id: str) -> Product:
    product = get_product_or_404(product_id)
    if not product.visual:
        raise HTTPException(
            status_code=400,
            detail=f"Product {product_id!r} is not a visual product",
        )
    return product


def parse_date_or_422(date: str) -> pd.Timestamp:
    try:
        return str_to_utc_timestamp(date)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid date: {date}") from e


def resolve_timestamp_or_404(product: Product, ts: pd.Timestamp) -> str:
    compact = compact_timestamp(ts)
    if compact not in product.timestamps and str(ts) not in product.timestamps:
        # also accept ISO stored timestamps
        from data_access_service.tiler.utils.dates import iso_timestamp

        iso = iso_timestamp(compact)
        if iso not in product.timestamps and compact not in product.timestamps:
            raise HTTPException(
                status_code=404,
                detail=f"Date {iso} is not available for product {product.id}",
            )
    return compact


def parse_rescale(rescale: str | None) -> tuple[float, float] | None:
    if rescale is None:
        return None
    try:
        lo, hi = (float(p.strip()) for p in rescale.split(","))
    except ValueError as e:
        raise HTTPException(status_code=400, detail="rescale must be 'min,max'") from e
    return lo, hi


def single_variable_or_400(product: Product, context: str = "") -> str:
    return product.variable
