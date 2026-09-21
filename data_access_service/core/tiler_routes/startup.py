"""Tiler startup: load the catalogue from ``root_metadata.json`` and each
store's ``metadata.json``, then mark the tiler ready.

Any failure, or every store failing to load, leaves it unready (503).
``refresh_catalog`` is also run by the scheduler, to pick up batch changes.
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
    ProductIdentity,
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
    prewarm_stores,
    retain_stores,
)
from data_access_service.tiler.utils.s3_json import read_json

logger = logging.getLogger(__name__)


def _load_catalog() -> dict[str, Product]:
    output_dir = Config.get_config().get_tiler_output_dir()
    root = RootMetadata.from_dict(read_json(root_metadata_path(output_dir)))
    identities = {
        entry["id"]: ProductIdentity.from_dict(entry) for entry in root.products
    }
    return build_catalog(identities)


def refresh_catalog() -> tuple[dict[str, Product], dict[str, BaseException | None]]:
    """Publish the products in ``root_metadata.json``, load the metadata of any
    store not loaded yet, and forget removed stores. Blocking (S3 reads).

    Returns ``(products, {store: None or the load error})``.
    """
    products = _load_catalog()
    load_products(products)
    stores = {product.store for product in products.values()}
    retain_stores(stores)
    outcomes = prewarm_stores(sorted(stores))
    return products, outcomes


async def run_tiler_warmup() -> None:
    try:
        products, outcomes = await anyio.to_thread.run_sync(
            refresh_catalog, limiter=TILE_THREAD_LIMITER
        )
        load_colormaps()
        await anyio.to_thread.run_sync(warmup_kernels, limiter=TILE_THREAD_LIMITER)
        await anyio.to_thread.run_sync(warmup_visual, limiter=TILE_THREAD_LIMITER)

        failed = sum(1 for outcome in outcomes.values() if outcome is not None)
        if failed == len(outcomes):
            raise RuntimeError(
                f"All {len(outcomes)} store(s) failed to load; refusing to "
                "mark the tiler ready with a catalogue that would 404 on "
                "every request"
            )

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
