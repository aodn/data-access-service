from fastapi import APIRouter

from data_access_service.config.config import Config
from data_access_service.core.routes.data import router as data_router
from data_access_service.core.routes.estimation_index import (
    router as estimation_index_router,
)
from data_access_service.core.routes.info import router as info_router
from data_access_service.core.routes.metadata import router as metadata_router
from data_access_service.core.routes.pmtiles import router as pmtiles_router

router = APIRouter(prefix=Config.BASE_URL)
router.include_router(info_router)
router.include_router(metadata_router)
router.include_router(data_router)
router.include_router(pmtiles_router)
router.include_router(estimation_index_router)
