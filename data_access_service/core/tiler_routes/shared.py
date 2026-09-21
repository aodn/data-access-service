"""Helpers shared by the tiler routers."""

from collections.abc import Callable
from http import HTTPStatus
from typing import TypeVar

import anyio
import pandas as pd
from fastapi import HTTPException, Request
from fastapi.openapi.models import Example

from data_access_service.config.config import Config
from data_access_service.tiler.services.colormap.resolver import resolve_colormap
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import get_product
from data_access_service.tiler.services.store.registry import (
    is_store_available,
    resolve_timestamp,
    unavailable_date_message,
)
from data_access_service.tiler.services.store.slice_loader import load_slice
from data_access_service.tiler.utils.dates import str_to_utc_timestamp
from data_access_service.tiler.utils.memory import trim_if_over_threshold

PRODUCT_EX: dict[str, Example] = {"default": Example(value="sea_level_anomaly")}
DATE_EX: dict[str, Example] = {"default": Example(value="2024-02-24T00:00:00Z")}

T = TypeVar("T")

TILE_THREAD_LIMITER = anyio.CapacityLimiter(
    Config.get_config().get_tiler_api_config().thread_pool_size
)

_DISCONNECT_POLL_INTERVAL = 0.1


class ClientDisconnected(Exception):
    """The client disconnected before the work finished."""


async def run_cancellable(request: Request, fn: Callable[[], T]) -> T:
    """Run ``fn`` in a tile thread, raising ClientDisconnected as soon as
    the client goes away. A queued ``fn`` then never runs; a running one
    finishes in the background and its result is dropped."""
    outcome: list[T] = []

    def _trim_then_run() -> T:
        # In the worker thread, so a queued request never trims and the
        # event loop never waits on it.
        trim_if_over_threshold()
        return fn()

    async def _runner(tg: anyio.abc.TaskGroup) -> None:
        outcome.append(
            await anyio.to_thread.run_sync(
                _trim_then_run, abandon_on_cancel=True, limiter=TILE_THREAD_LIMITER
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
    """503 until tiler startup has finished."""
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


def is_store_available_or_404(product: Product) -> None:
    """404 if the product's store failed to load."""
    if not is_store_available(product.store):
        raise HTTPException(
            status_code=404,
            detail=f"Product {product.id!r} is temporarily unavailable: its store failed to open",
        )


def visual_product_or_400(product_id: str) -> Product:
    """The product, or 400 if it has no visual tiles."""
    product = get_product_or_404(product_id)
    if not product.visual:
        raise HTTPException(
            status_code=400,
            detail=f"Product {product_id!r} does not support visual tiles",
        )
    return product


def parse_date_or_422(date: str) -> pd.Timestamp:
    """Parse ``date`` as a UTC timestamp, or 422."""
    try:
        return str_to_utc_timestamp(date, require_tz=True)
    except ValueError as e:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid date: {date!r} ({e}) — expected a full UTC "
                "timestamp (e.g. '2024-06-15T23:00:00Z'). Use one of the "
                "exact values from /manifest's available_dates."
            ),
        ) from e


def resolve_timestamp_or_404(product: Product, ts: pd.Timestamp) -> None:
    """404 early if ``ts`` isn't one of the store's timestamps."""
    if resolve_timestamp(product.store, ts) is None:
        raise HTTPException(
            status_code=404,
            detail=unavailable_date_message(product.store, ts),
        )


def load_slice_or_404(
    store: str, ts: pd.Timestamp, variables: list[str], ocean_masked: bool = False
):
    try:
        return load_slice(store, ts, variables, ocean_masked)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


def resolve_colormap_or_error(name: str, *, status_code: int = 400) -> None:
    """Raise ``status_code`` (400, or 404 for a path segment) for an unknown
    colormap."""
    try:
        resolve_colormap(name)
    except ValueError as e:
        raise HTTPException(status_code=status_code, detail=str(e)) from e


def single_variable_or_400(product: Product, *, context: str) -> str:
    """The product's single variable, or 400 for a variable pair."""
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
