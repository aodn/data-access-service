import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from data_access_service import Config
from data_access_service.config.config import EnvType
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router


def api_setup(application: FastAPI) -> API:
    """
    This function is not async which can be use in test, the lifespan however
    expect async function which is not good for testing
    :param application:
    :return:
    """
    config = Config

    # Ensure we only create one instance of the API
    # for testing, multiple api instance is necessary to make sure they are independent
    if hasattr(application.state, "api_instance") and config.get_env_type() != EnvType.TESTING:  # type: ignore
        return application.state.api_instance  # type: ignore
    api = API()
    application.state.api_instance = api  # type: ignore

    # Heavy load so try to use a task to complete it in the background
    try:
        asyncio.create_task(api.async_initialize_metadata())
    except Exception as e:
        api.initialize_metadata()

    application.include_router(api_router)
    return api


@asynccontextmanager
async def lifespan(application: FastAPI):
    api = api_setup(application)
    yield
    api.destroy()


app = FastAPI(lifespan=lifespan, title="Data Access Service")


# reload = True is used for faster local development. It cannot be used with workers > 1
# If workers=1, the api may only working in one thread. So for local running, please change
# the settings according to your needs. Appdeploy has disabled reload and set workers=3
if __name__ == "__main__":
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run(
        "data_access_service.server:app",
        host="0.0.0.0",
        port=5000,
        # reload=True,
        workers=3,
        log_config=log_config_path,
        timeout_keep_alive=900,  # 15 mins
    )
