"""Tiler warmup sequence run during app startup, once API metadata is ready.

Nothing is published before it is verified, and every fatal path exits without
``mark_tiler_ready()`` — so the failure mode is a 503 behind
``require_tiler_ready``, never a catalogue that is quietly wrong.
"""

import asyncio
import logging

import anyio

from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.tiler.schemas.gridded_variables import load_gridded_variables
from data_access_service.tiler.services.colormap.registry import load_colormaps
from data_access_service.tiler.services.product.discovery import (
    build_candidate_products,
    log_unmatched_overrides,
)
from data_access_service.tiler.services.product.registry import publish_products
from data_access_service.tiler.services.product.verification import (
    verify_candidate_products,
)
from data_access_service.tiler.services.rendering.kernels import warmup_resample
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import prewarm_stores

logger = logging.getLogger(__name__)


async def run_tiler_warmup(api: API) -> None:
    """Derive, verify, and publish the product catalogue, then mark ready."""
    try:
        logger.info("Waiting for API metadata init before starting other tasks")
        # Indefinite: deriving from a half-populated index would publish a
        # silently incomplete catalogue.
        if not await api.wait_until_ready(timeout=None):
            raise RuntimeError("API metadata never became ready")

        entries = load_gridded_variables()
        candidates = build_candidate_products(
            api.get_dataset_variables(None),
            entries,
            Config.get_config().get_tiler_config().zarr_store_base_url,
        )
        log_unmatched_overrides(candidates, entries)

        load_colormaps()
        await anyio.to_thread.run_sync(warmup_resample)
        await anyio.to_thread.run_sync(warmup_visual)

        outcomes = await prewarm_stores(
            sorted({product.source_path for product in candidates.values()})
        )
        result = verify_candidate_products(candidates, outcomes)
        result.log_rejections()
        if not result.products:
            raise RuntimeError("No tiler-compatible products were discovered")

        publish_products(result.products)
        mark_tiler_ready()
        logger.info(
            "Tiler ready: %d products from %d stores (%d candidates dropped)",
            len(result.products),
            len({p.source_path for p in result.products.values()}),
            len(result.rejections),
        )
    except asyncio.CancelledError:
        # Shutdown; without this it would be logged as a warmup failure below.
        raise
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
