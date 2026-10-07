"""Tiler startup: load the catalogue from ``root_metadata.json`` and each
store's ``metadata.json``, then mark the tiler ready.
"""

import asyncio
import logging
import threading
from datetime import timedelta

from anyio.to_thread import run_sync
from tenacity import retry, wait_exponential

from data_access_service.config.config import Config
from data_access_service.core.tiler_routes.shared import (
    TILE_THREAD_LIMITER,
    mark_tiler_ready,
)
from data_access_service.models.tiler_parquet_types import (
    RootMetadata,
    root_metadata_path,
)
from data_access_service.tiler.services.colormap.registry import load_colormaps
from data_access_service.tiler.services.product.catalog import build_catalog
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import load_products
from data_access_service.tiler.services.rendering.kernels import warmup_kernels
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import (
    load_stores,
    refresh_stores,
    retain_stores,
)
from data_access_service.utils.retry_utils import log_retry_attempt
from data_access_service.utils.s3_json import read_json

logger = logging.getLogger(__name__)

# Catalogue reads hit S3 at process start. Retry until one succeeds; the
# ready flag stays false, and tiler routes stay 503, across the waits.
_CATALOG_MIN_WAIT = timedelta(seconds=2)
_CATALOG_MAX_WAIT = timedelta(minutes=60)


def _load_catalog() -> dict[str, Product]:
    tiler_root_dir = Config.get_config().get_tiler_root_dir()
    root = RootMetadata.from_dict(read_json(root_metadata_path(tiler_root_dir)))
    return build_catalog({product.id: product for product in root.products})


def refresh_catalog() -> tuple[dict[str, Product], dict[str, BaseException | None]]:
    """Publish the products in ``root_metadata.json``, load the metadata of any
    store not loaded yet, and forget removed stores. Blocking (S3 reads).

    Returns ``(products, {store: None or the load error})``.
    """
    products = _load_catalog()
    stores = {product.store for product in products.values()}
    retain_stores(stores)
    outcomes = load_stores(sorted(stores))
    # A store job writes metadata.json only after it converts. Until then the
    # sidecar is missing; leave those products out instead of advertising them.
    missing = {
        store
        for store, error in outcomes.items()
        if isinstance(error, FileNotFoundError)
    }
    if missing:
        skipped = sorted(
            pid for pid, product in products.items() if product.store in missing
        )
        logger.warning(
            "Skipping %d product(s) with no metadata.json: %s",
            len(skipped),
            skipped,
        )
        products = {
            pid: product
            for pid, product in products.items()
            if product.store not in missing
        }
    if products:
        load_products(products)
    else:
        logger.warning("No products left to publish; catalogue unchanged")
    return products, outcomes


class RefreshInProgressError(Exception):
    """A tiler refresh is already running."""


_refresh_lock = threading.Lock()


def refresh_tiler() -> tuple[dict[str, Product], dict[str, BaseException | None]]:
    """Re-read every loaded store's metadata.json, then root_metadata.json."""
    if not _refresh_lock.acquire(blocking=False):
        raise RefreshInProgressError("Tiler refresh is already in progress")
    try:
        refresh_stores()
        return refresh_catalog()
    finally:
        _refresh_lock.release()


# Bug in tenacity, the type check always fail but function ok
# noinspection PyCallingNonCallable
@retry(
    wait=wait_exponential(multiplier=1, min=_CATALOG_MIN_WAIT, max=_CATALOG_MAX_WAIT),
    before_sleep=log_retry_attempt("Tiler catalogue load", logger),
    reraise=True,
)
async def _refresh_catalog_with_retry() -> (
    tuple[dict[str, Product], dict[str, BaseException | None]]
):
    """Load the catalogue off the event loop, retrying until it succeeds."""
    return await run_sync(refresh_catalog, limiter=TILE_THREAD_LIMITER)


async def run_tiler_warmup() -> None:
    """Publish the catalogue, open tiler routes, then warm the render path.

    Catalogue reads retry until one succeeds. Colormap loading and kernel
    warmup run once after that, so a numba or GDAL failure cannot put the
    routes back to 503.
    """
    products, outcomes = await _refresh_catalog_with_retry()
    failed = sum(error is not None for error in outcomes.values())
    mark_tiler_ready()
    logger.info(
        "Tiler ready: %d products from %d stores (%d store(s) failed to load)",
        len(products),
        len(outcomes),
        failed,
    )
    try:
        load_colormaps()
        await run_sync(warmup_kernels, limiter=TILE_THREAD_LIMITER)
        await run_sync(warmup_visual, limiter=TILE_THREAD_LIMITER)
    except asyncio.CancelledError:
        raise  # shutdown
    except Exception:
        logger.exception("Tiler render warmup failed")
