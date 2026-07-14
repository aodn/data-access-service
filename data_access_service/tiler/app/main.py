import logging
from contextlib import asynccontextmanager

import anyio
import starlette.middleware.gzip as _gzip_mw
from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware

from data_access_service.tiler.app.config import settings
from data_access_service.tiler.app.routers.data_tiles import router as data_tiles_router
from data_access_service.tiler.app.routers.visual_tiles import (
    router as visual_tiles_router,
)
from data_access_service.tiler.app.services.colormap.registry import load_colormaps
from data_access_service.tiler.app.services.product.registry import (
    iter_products,
    load_products,
)
from data_access_service.tiler.app.services.rendering.kernels import warmup_resample
from data_access_service.tiler.app.services.rendering.visual_tiles import warmup_visual
from data_access_service.tiler.app.services.store.registry import prewarm_stores

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = settings.THREAD_POOL_SIZE
    logger.info(f"Thread pool size set: {limiter.total_tokens}")
    load_products()
    load_colormaps()

    await anyio.to_thread.run_sync(warmup_resample)
    await anyio.to_thread.run_sync(warmup_visual)
    store_urls = list({p.source_path for p in iter_products()})

    async with anyio.create_task_group() as tg:
        tg.start_soon(prewarm_stores, store_urls)
        try:
            yield
        finally:
            logger.info("Shutting down")
            tg.cancel_scope.cancel()


app = FastAPI(
    title="IMOS Tile Server",
    lifespan=lifespan,
)


if "image/" not in _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES:
    _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES += ("image/",)  # type: ignore[assignment]
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

app.include_router(data_tiles_router, prefix="/tiler/data_tiles", tags=["data_tiles"])
app.include_router(
    visual_tiles_router, prefix="/tiler/visual_tiles", tags=["visual_tiles"]
)
