from contextlib import asynccontextmanager

from fastapi import FastAPI
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router
from data_access_service.config.config import (
    EnvType,
    DevConfig,
    StagingConfig,
    EdgeConfig,
    ProdConfig,
    TestConfig,
)

import os

@asynccontextmanager
async def lifespan(application: FastAPI):
    profile = EnvType(os.getenv("PROFILE", EnvType.DEV))
    match profile:
        case EnvType.PRODUCTION:
            application.state.config = ProdConfig() # type: ignore

        case EnvType.EDGE:
            application.state.config = EdgeConfig() # type: ignore

        case EnvType.STAGING:
            application.state.config = StagingConfig() # type: ignore

        case EnvType.TESTING:
            application.state.config = TestConfig() # type: ignore

        case _:
            application.state.config = DevConfig() # type: ignore

    application.state.api_instance = API() # type: ignore
    application.include_router(api_router)

    yield

app = FastAPI(lifespan=lifespan, title="Data Access Service")
