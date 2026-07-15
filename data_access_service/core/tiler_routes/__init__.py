from fastapi import APIRouter, Depends

from data_access_service.config.config import Config
from data_access_service.core.tiler_routes.data_tiles import router as data_tiles_router
from data_access_service.core.tiler_routes.shared import require_tiler_ready
from data_access_service.core.tiler_routes.visual_tiles import (
    router as visual_tiles_router,
)
from data_access_service.utils.api_utils import api_key_auth

router = APIRouter(
    prefix=Config.BASE_URL,
    dependencies=[Depends(api_key_auth), Depends(require_tiler_ready)],
)
router.include_router(
    data_tiles_router, prefix="/tiler/data_tiles", tags=["data_tiles"]
)
router.include_router(
    visual_tiles_router, prefix="/tiler/visual_tiles", tags=["visual_tiles"]
)
