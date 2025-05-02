from contextlib import asynccontextmanager

from fastapi import FastAPI
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router


@asynccontextmanager
async def lifespan(application: FastAPI):
    application.state.api_instance = API()  # type: ignore
    application.include_router(api_router)

    yield


app = FastAPI(lifespan=lifespan, title="Data Access Service")
