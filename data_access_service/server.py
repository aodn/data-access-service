from fastapi import FastAPI
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.config.config import (
    EnvType,
    DevConfig,
    StagingConfig,
    EdgeConfig,
    ProdConfig,
)

import os


def create_app() -> FastAPI:
    app = FastAPI()
    profile = EnvType(os.getenv("PROFILE", EnvType.DEV))
    if profile == EnvType.PRODUCTION:
        app.state.config = ProdConfig()
    elif profile == EnvType.EDGE:
        app.state.config = EdgeConfig()
    elif profile == EnvType.STAGING:
        app.state.config = StagingConfig()
    else:
        app.state.config = DevConfig()

    app.state.api_instance = API()
    app.include_router(api_router)
    return app


app = create_app()
