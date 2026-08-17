import os

from http import HTTPStatus
from fastapi import APIRouter, Request
from data_access_service.core.routes.helpers import (
    HealthCheckResponse,
    get_api_instance,
)

VERSION = os.getenv("APP_VERSION", "main-SNAPSHOT")
GIT_SHA = os.getenv("GIT_SHA")

router = APIRouter()


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(request: Request):
    """
    Health check endpoint. The init now become very slow due to the need to load zarr data on init
    so we report status code OK, to avoid AWS timeout but the status value is STARTING

    This endpoint will not be call in Docker run due to fact that we install a Ngnix in front to
    intercept call of health. The reason is during heavy load, the health may not reply on time
    and cause AWS kill process. The Ngnix will reply health check using a json that is dump by
    this process.
    """
    api_instance = get_api_instance(request)
    if api_instance.get_api_status():
        return HealthCheckResponse(status="UP", status_code=HTTPStatus.OK)
    else:
        return HealthCheckResponse(status="STARTING", status_code=HTTPStatus.OK)


@router.get("/manage/info")
def get_info():
    return {
        "application": {
            "name": "data-access-service",
            "description": "Data Access Service",
            "version": VERSION,
        },
        "git": {"commit": {"id": GIT_SHA}},
    }
