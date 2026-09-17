"""Tiler warmup, run at app startup from the batch-published catalogue.

Every product listed in ``root_metadata.json`` is published immediately —
nothing waits on its store's sidecar loading. Per-store health lands in
``store.registry`` (via ``prewarm_stores``) and is enforced per-request from
there, not by withholding a product from the registry. The one thing that
still keeps the tiler unready is every store failing to load, which would
serve a catalogue that 404s on every request. Every other fatal path also
exits without ``mark_tiler_ready()`` — the failure mode is a 503, never a
wrong catalogue.

No live API metadata and no zarr here — the whole catalogue comes from
``root_metadata.json`` + each store's ``metadata.json`` sidecar, both
published by the batch conversion job (``batch.tiler.generator``).
"""

import asyncio
import json
import logging
import os

import anyio

from data_access_service.config.config import Config
from data_access_service.core.tiler_routes.shared import (
    TILE_THREAD_LIMITER,
    mark_tiler_ready,
)
from data_access_service.models.tiler_parquet_types import RootMetadata
from data_access_service.tiler.services.colormap.registry import load_colormaps
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import load_products
from data_access_service.tiler.services.rendering.kernels import warmup_resample
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import prewarm_stores

logger = logging.getLogger(__name__)


def _load_root_metadata() -> dict[str, Product]:
    output_dir = Config.get_config().get_tiler_parquet_config().output_dir
    path = os.path.join(output_dir, "root_metadata.json")
    with open(path) as f:
        root = RootMetadata.from_dict(json.load(f))
    return {p.id: p for p in (Product.from_dict(entry) for entry in root.products)}


async def run_tiler_warmup() -> None:
    try:
        products = _load_root_metadata()

        load_products(products)
        load_colormaps()
        await anyio.to_thread.run_sync(warmup_resample, limiter=TILE_THREAD_LIMITER)
        await anyio.to_thread.run_sync(warmup_visual, limiter=TILE_THREAD_LIMITER)

        outcomes = await prewarm_stores(
            sorted({product.source_path for product in products.values()})
        )
        if all(outcome is not None for outcome in outcomes.values()):
            raise RuntimeError(
                f"All {len(outcomes)} store(s) failed to load; refusing to "
                "mark the tiler ready with a catalogue that would 404 on "
                "every request"
            )

        mark_tiler_ready()
        failed = sum(1 for outcome in outcomes.values() if outcome is not None)
        logger.info(
            "Tiler ready: %d products from %d stores (%d store(s) failed to load)",
            len(products),
            len(outcomes),
            failed,
        )
    except asyncio.CancelledError:
        raise  # shutdown, not a warmup failure
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
