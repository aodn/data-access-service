"""Tiler warmup sequence run during app startup, once API metadata is ready."""

import logging

import anyio

from data_access_service.core.api import API
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.tiler.services.colormap.registry import load_colormaps
from data_access_service.tiler.services.product.registry import (
    iter_products,
    load_products,
)
from data_access_service.tiler.services.rendering.kernels import warmup_resample
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import prewarm_stores
from data_access_service.utils.api_utils import wait_until_api_ready

logger = logging.getLogger(__name__)


async def run_tiler_warmup(api: API) -> None:
    """Load products/colormaps, warm up rendering kernels, and prewarm stores.

    Waits for API metadata init to finish first so this doesn't compete with
    it for CPU/memory.
    """
    logger.info("Waiting for API metadata init before starting other tasks")
    await wait_until_api_ready(api)

    load_products()
    load_colormaps()
    await anyio.to_thread.run_sync(warmup_resample)
    await anyio.to_thread.run_sync(warmup_visual)

    store_urls = list({p.source_path for p in iter_products()})
    await prewarm_stores(store_urls)
    mark_tiler_ready()
