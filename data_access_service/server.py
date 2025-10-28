import asyncio
from asyncio import AbstractEventLoop
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.core.scheduler import TaskScheduler


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

    # Heavy load so try to use a task to complete it in the background
    try:
        if not isinstance(Config.get_config(), IntTestConfig):
            # Check for running event loop first to avoid creating an unawaited coroutine
            loop: AbstractEventLoop = asyncio.get_running_loop()
            asyncio.create_task(
                api.async_initialize_metadata(), name="api_metadata_init"
            )
        else:
            api.initialize_metadata()
    except Exception as e:
        api.initialize_metadata()

    application.include_router(api_router)
    return api


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Initialize API
    api = api_setup(application)

    # Initialize and start scheduler
    scheduler = TaskScheduler()
    await scheduler.start_with_initial_run()  # Runs task on startup + schedules hourly
    application.state.scheduler = scheduler  # type: ignore

    yield

    # Cleanup
    scheduler.shutdown()
    api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")


if __name__ == "__main__":
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=5000,
        reload=True,
        workers=1,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
