from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from data_access_service.core.api import API
from data_access_service.core.routes import router as api_router


@asynccontextmanager
async def lifespan(application: FastAPI):
    application.state.api_instance = API()  # type: ignore
    application.include_router(api_router)

    yield


app = FastAPI(lifespan=lifespan, title="Data Access Service")


if __name__ == "__main__":
    log_config_path = str(Path(__file__).parent.parent / "log_config.yaml")
    uvicorn.run("data_access_service.server:app",
                host="0.0.0.0",
                port=8000,
                reload=True,
                log_config=log_config_path)