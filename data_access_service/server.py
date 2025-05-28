import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from data_access_service.core.api import API

from data_access_service.core.routes import router as api_router


def api_setup(application: FastAPI):
    """
    This function is not async which can be use in test, the lifespan however
    expect async function which is not good for testing
    :param application:
    :return:
    """
    api = API()
    application.state.api_instance = api  # type: ignore

    # Heavy load so try to use a task to complete it in the background
    try:
        asyncio.create_task(api.async_initialize_metadata())
    except Exception as e:
        api.initialize_metadata()

    application.include_router(api_router)


@asynccontextmanager
async def lifespan(application: FastAPI):
    api_setup(application)
    yield


app = FastAPI(lifespan=lifespan, title="Data Access Service")


if __name__ == "__main__":
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=5000,
        reload=True,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
