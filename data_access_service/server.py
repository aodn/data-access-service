import asyncio
import logging
import os
import uvicorn

import anyio
import starlette.middleware.gzip as _gzip_mw
from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.core.scheduler import TaskScheduler
from data_access_service.core.tiler_routes import router as tiler_router
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.sites.sites_repository import build_repositories
from data_access_service.core.duckdbclient import ParquetDuckDBClient
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


def api_setup(application: FastAPI) -> API:
    """
    This function is not async which can be use in test, the lifespan however
    expect async function which is not good for testing
    :param asynchronize:
    :param application:
    :return:
    """
    api = API()
    application.state.api_instance = api  # type: ignore
    application.state.repositories = {}  # type: ignore

    # Heavy load so try to use a task to complete it in the background
    try:
        if not isinstance(Config.get_config(), IntTestConfig):
            # Check for running event loop first to avoid creating an unawaited coroutine
            asyncio.get_running_loop()
            asyncio.create_task(
                api.async_initialize_metadata(), name="api_metadata_init"
            )
        else:
            api.initialize_metadata()
    except Exception:
        api.initialize_metadata()

    application.include_router(api_router)
    application.include_router(tiler_router)
    return api


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Initialize API
    api = api_setup(application)

    session = None
    scheduler = None
    if isinstance(Config.get_config(), IntTestConfig):
        yield
    else:
        session = ParquetDuckDBClient()
        application.state.duckdb_session = session
        application.state.repositories = build_repositories(session)
        scheduler = TaskScheduler(api, application.state.repositories)

        limiter = anyio.to_thread.current_default_thread_limiter()
        limiter.total_tokens = Config.get_config().get_tiler_config().thread_pool_size

        async def _tiler_startup_after_metadata_init() -> None:
            logger.info("Waiting for API metadata init before starting other tasks")
            await wait_until_api_ready(api)

            asyncio.create_task(
                scheduler.start_with_initial_run(), name="repository_cache"
            )

            load_products()
            load_colormaps()
            await anyio.to_thread.run_sync(warmup_resample)
            await anyio.to_thread.run_sync(warmup_visual)
            mark_tiler_ready()

            store_urls = list({p.source_path for p in iter_products()})
            await prewarm_stores(store_urls)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_tiler_startup_after_metadata_init)
            try:
                yield
            finally:
                logger.info("Shutting down tiler prewarm")
                tg.cancel_scope.cancel()

    # Cleanup
    if scheduler:
        scheduler.shutdown()
    if session:
        session.close()
    api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")

# PNG/WebP tiles are already compressed; gzipping them wastes CPU. Core JSON
# responses that already set their own Content-Encoding (see
# utils/routes_helper.py's gzip_compress) are left untouched by this
# middleware — it skips compression when content-encoding is already set.
if "image/" not in _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES:
    _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES += ("image/",)  # type: ignore[assignment]
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


if __name__ == "__main__":
    # Turn off reload by default, else production will pick set reload true
    reload_mode = os.getenv("FASTAPI_RELOAD", "false").lower() == "true"
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=5000,
        reload=reload_mode,
        workers=1,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
