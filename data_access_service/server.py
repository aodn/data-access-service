import asyncio
import os
import secrets
import uvicorn

from fastapi import FastAPI
from contextlib import asynccontextmanager
from pathlib import Path
from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.core.scheduler import TaskScheduler
from data_access_service.models.duckdb_repository import build_repositories
from data_access_service.models.duckdb_session import DuckDBSession


def _build_duckdb_session() -> DuckDBSession:
    """Build the single on-disk DuckDB session shared by every repository.

    On disk (not ``:memory:``) with a memory limit and a temp dir so large
    dataset loads spill to disk instead of OOM-killing the container. A random
    db path lets multiple instances run locally without clashing.
    """
    config = Config.get_config()
    temp_dir = os.path.join(os.getcwd(), ".duckdb_temp")
    os.makedirs(temp_dir, exist_ok=True)
    db_path = f"/tmp/data_access_{secrets.token_urlsafe(16)}.duckdb"

    session = DuckDBSession(database=db_path)
    session.execute("SET GLOBAL threads = 1")
    session.execute(f"SET GLOBAL memory_limit = '{config.get_duckdb_maxmem()}'")
    session.execute(f"SET GLOBAL temp_directory = '{temp_dir}'")
    return session


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

    # Build the DuckDB session + repositories and start the scheduler (skip in
    # test environment, which has no AWS credentials for the S3 secrets).
    session = None
    scheduler = None
    if not isinstance(Config.get_config(), IntTestConfig):
        session = _build_duckdb_session()
        application.state.duckdb_session = session  
        application.state.repositories = build_repositories(session)  
        scheduler = TaskScheduler(api, application.state.repositories)
        # Check for running event loop first to avoid creating an unawaited coroutine
        asyncio.create_task(
            scheduler.start_with_initial_run(), name="repository_cache"
        )

    yield

    # Cleanup
    if scheduler:
        scheduler.shutdown()
    if session:
        session.close()
    api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")


if __name__ == "__main__":
    # Turn off reload by default, else production will pick set reload true
    reload_mode = os.getenv("FASTAPI_RELOAD", "false").lower() == "true"
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=5001,
        reload=reload_mode,
        workers=1,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
