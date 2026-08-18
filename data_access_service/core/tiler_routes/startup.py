"""Tiler warmup, run during app startup once API metadata is ready.

Every discovered candidate is published immediately — nothing waits on its
store opening. Per-store health lands in ``store.registry`` (via
``prewarm_stores``) and is enforced per-request from there, not by
withholding a product from the registry. The one thing that still keeps the
tiler unready is every store failing to open: a catalogue that would 404 on
every single request is exactly the "quietly wrong" outcome this guards
against. Every other fatal path also exits without ``mark_tiler_ready()`` —
the failure mode is a 503, never a wrong catalogue.
"""

import asyncio
import logging

import anyio

from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.tiler.services.colormap.registry import load_colormaps
from data_access_service.tiler.services.product.discovery import discover_products
from data_access_service.tiler.services.product.registry import load_products
from data_access_service.tiler.services.rendering.kernels import warmup_resample
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import prewarm_stores

logger = logging.getLogger(__name__)


async def run_tiler_warmup(api: API) -> None:
    try:
        logger.info("Waiting for API metadata init before starting other tasks")
        # Indefinite: a half-populated index would publish a partial catalogue.
        if not await api.wait_until_ready(timeout=None):
            raise RuntimeError("API metadata never became ready")

        products = discover_products(
            api, Config.get_config().get_tiler_config().co_bucket
        )
        load_products(products)
        load_colormaps()
        await anyio.to_thread.run_sync(warmup_resample)
        await anyio.to_thread.run_sync(warmup_visual)

        outcomes = await prewarm_stores(
            sorted({product.source_path for product in products.values()})
        )
        if all(outcome is not None for outcome in outcomes.values()):
            raise RuntimeError(
                f"All {len(outcomes)} store(s) failed to open; refusing to "
                "mark the tiler ready with a catalogue that would 404 on "
                "every request"
            )

        mark_tiler_ready()
        failed = sum(1 for outcome in outcomes.values() if outcome is not None)
        logger.info(
            "Tiler ready: %d products from %d stores (%d store(s) failed to open)",
            len(products),
            len(outcomes),
            failed,
        )
    except asyncio.CancelledError:
        raise  # shutdown, not a warmup failure
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
