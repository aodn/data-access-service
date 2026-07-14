import asyncio
import os
import uvicorn

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from contextlib import asynccontextmanager
from pathlib import Path
from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.core.scheduler import TaskScheduler
from data_access_service.sites.sites_repository import build_repositories
from data_access_service.core.duckdbclient import ParquetDuckDBClient
from data_access_service.tiler.app.main import app as tiler_app


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
    return api


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Initialize API
    api = api_setup(application)
    # Mount tiler app
    application.mount(Config.BASE_URL, tiler_app)

    session = None
    scheduler = None
    if isinstance(Config.get_config(), IntTestConfig):
        yield
    else:
        session = ParquetDuckDBClient()
        application.state.duckdb_session = session
        application.state.repositories = build_repositories(session)
        scheduler = TaskScheduler(api, application.state.repositories)
        asyncio.create_task(scheduler.start_with_initial_run(), name="repository_cache")
        # Inject tiler app lifespan
        async with tiler_app.router.lifespan_context(tiler_app):
            yield

    # Cleanup
    if scheduler:
        scheduler.shutdown()
    if session:
        session.close()
    api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")


def custom_openapi():
    """
    The tiler is mounted as its own FastAPI app via `application.mount(...)`, so its
    routes live in a separate ASGI app and are invisible to this app's generated
    OpenAPI schema by default. Merge the tiler's schema in (path-prefixed by
    Config.BASE_URL, matching where the mount actually serves them) so `/docs` shows
    every endpoint in one place.
    """
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    tiler_schema = tiler_app.openapi()

    schema.setdefault("paths", {})
    for path, path_item in tiler_schema.get("paths", {}).items():
        schema["paths"][f"{Config.BASE_URL}{path}"] = path_item

    for section, entries in tiler_schema.get("components", {}).items():
        schema.setdefault("components", {}).setdefault(section, {}).update(entries)

    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = custom_openapi


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
