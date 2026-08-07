"""Tiler warmup sequence run during app startup, once API metadata is ready.

Sequencing is the point of this module. Nothing is published before it has been
verified, and every fatal path exits without ``mark_tiler_ready()`` — so the
failure mode is a 503 behind ``require_tiler_ready``, never a tiler serving a
catalogue that is quietly wrong.
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
    reject_unmatched_overrides,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import publish_products
from data_access_service.tiler.services.product.verification import (
    LEGACY_PRODUCT_IDS,
    verify_candidate_products,
)
from data_access_service.tiler.services.rendering.kernels import warmup_resample
from data_access_service.tiler.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.services.store.registry import prewarm_stores

logger = logging.getLogger(__name__)


def assert_legacy_products_intact(products: dict[str, Product]) -> None:
    """Fail startup unless the five pre-derivation products survived intact.

    Pinning these IDs in a unit test proves the derivation *formula* is right;
    it cannot prove the five products survived a given boot. Those are different
    failures. A variable can still match other datasets while its original
    dataset is dropped — by a rename, a metadata gap, a failed store open, a
    rejected guard — leaving a large, healthy-looking catalogue that boots ready
    with a live product silently missing from the portal. Making that class of
    failure loud is the whole point of the redesign.

    Their two correctness-bearing settings are checked too, since both come from
    config paths this design rewrote.
    """
    missing = sorted(LEGACY_PRODUCT_IDS - products.keys())
    if missing:
        raise RuntimeError(
            "Products already in production use are missing from the derived "
            f"catalogue: {', '.join(missing)}"
        )

    gsla = products["model_sea_level_anomaly_gridded_realtime:gsla"]
    coastal_fill = gsla.data_tile.coastal_fill
    if coastal_fill is None or coastal_fill.max_dist_px != 4:
        raise RuntimeError(
            "model_sea_level_anomaly_gridded_realtime:gsla lost its "
            f"data_tile.coastal_fill (max_dist_px=4); got {coastal_fill!r}"
        )

    currents = products["model_sea_level_anomaly_gridded_realtime:ucur+vcur"]
    if not currents.ocean_masked:
        raise RuntimeError(
            "model_sea_level_anomaly_gridded_realtime:ucur+vcur lost ocean_masked; "
            "the dataset override that carries it no longer applies."
        )


async def run_tiler_warmup(api: API) -> None:
    """Derive, verify, and publish the product catalogue, then mark ready.

    Runs as a named lifespan task whose result is never awaited, so it has to
    report its own failures: anything unexpected is logged at CRITICAL and the
    tiler simply stays unready.
    """
    try:
        logger.info("Waiting for API metadata init before starting other tasks")
        # Indefinite: the catalogue is derived from the metadata index, so
        # deriving from a half-populated one would publish a silently
        # incomplete catalogue — strictly worse than becoming ready later.
        if not await api.wait_until_ready(timeout=None):
            raise RuntimeError("API metadata never became ready")

        entries = load_gridded_variables()
        candidates = build_candidate_products(
            api.get_dataset_variables(None),
            entries,
            Config.get_config().get_tiler_config().zarr_store_base_url,
        )
        reject_unmatched_overrides(candidates, entries)

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
        assert_legacy_products_intact(result.products)

        publish_products(result.products)
        mark_tiler_ready()
        logger.info(
            "Tiler ready: %d products from %d stores (%d candidates dropped)",
            len(result.products),
            len({p.source_path for p in result.products.values()}),
            len(result.rejections),
        )
    except asyncio.CancelledError:
        # Shutdown cancels this task. Without the re-raise it would be swallowed
        # by the handler below and logged as a warmup failure.
        raise
    except Exception:
        logger.critical("Tiler warmup failed; tiler remains unready", exc_info=True)
