"""Tiler startup: load the catalogue from ``root_metadata.json`` and each
store's ``metadata.json``, then mark the tiler ready.
"""

import asyncio
import logging

import anyio

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
    retain_stores,
)
from data_access_service.utils.s3_json import read_json

logger = logging.getLogger(__name__)


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
    load_products(products)
    return products, outcomes


async def run_tiler_warmup() -> None:
    try:
        products, outcomes = await anyio.to_thread.run_sync(
            refresh_catalog, limiter=TILE_THREAD_LIMITER
        )
        load_colormaps()
        await anyio.to_thread.run_sync(warmup_kernels, limiter=TILE_THREAD_LIMITER)
        await anyio.to_thread.run_sync(warmup_visual, limiter=TILE_THREAD_LIMITER)

        failed = sum(error is not None for error in outcomes.values())
        mark_tiler_ready()
        logger.info(
            "Tiler ready: %d products from %d stores (%d store(s) failed to load)",
            len(products),
            len(outcomes),
            failed,
        )
    except asyncio.CancelledError:
        raise  # shutdown
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
