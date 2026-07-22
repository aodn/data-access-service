import asyncio
import logging
import os
import uvicorn

import anyio
from fastapi import FastAPI
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.api import API
from data_access_service.core.middleware import configure_gzip_middleware
from data_access_service.core.routes import router as api_router
from data_access_service.core.scheduler import TaskScheduler
from data_access_service.core.tiler_routes import router as tiler_router
from data_access_service.core.tiler_routes.startup import run_tiler_warmup
from data_access_service.sites.sites_repository import build_repositories
from data_access_service.core.duckdbclient import ParquetDuckDBClient

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
    background_tasks: tuple[asyncio.Task, ...] = ()
    try:
        if isinstance(Config.get_config(), IntTestConfig):
            yield
        else:
            session = ParquetDuckDBClient()
            application.state.duckdb_session = session
            application.state.repositories = build_repositories(session)
            scheduler = TaskScheduler(api, application.state.repositories)
            repository_cache_task = asyncio.create_task(
                scheduler.start_with_initial_run(), name="repository_cache"
            )
            tiler_warmup_task = asyncio.create_task(
                run_tiler_warmup(api), name="tiler_warmup"
            )
            background_tasks = (repository_cache_task, tiler_warmup_task)
            # Set the thread pool size for tiler endpoints to the configured value, as only the tiler endpoints use anyio thread pool.
            limiter = anyio.to_thread.current_default_thread_limiter()
            limiter.total_tokens = (
                Config.get_config().get_tiler_config().thread_pool_size
            )

            yield
    finally:
        logger.info("Shutting down background startup tasks")
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            with suppress(asyncio.CancelledError):
                await task

        # Cleanup
        if scheduler:
            scheduler.shutdown()
        if session:
            session.close()
        api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")
configure_gzip_middleware(app)


if __name__ == "__main__":
    # Turn off reload by default, else production will pick set reload true
    reload_mode = os.getenv("FASTAPI_RELOAD", "false").lower() == "true"
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=8000,
        reload=reload_mode,
        workers=1,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
