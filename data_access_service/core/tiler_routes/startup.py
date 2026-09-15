"""Tiler warmup: scan parquet metas and mark the visual tiler ready."""

import asyncio
import logging

from data_access_service.core.api import API
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.tiler.catalog import refresh_catalog
from data_access_service.tiler.colormap import load_colormaps

logger = logging.getLogger(__name__)


async def run_tiler_warmup(api: API) -> None:
    try:
        logger.info("Waiting for API metadata init before starting tiler catalog")
        if not await api.wait_until_ready(timeout=None):
            raise RuntimeError("API metadata never became ready")

        load_colormaps()
        products = refresh_catalog()
        mark_tiler_ready()
        logger.info("Tiler ready: %d parquet-backed product(s)", len(products))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
