from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request

from data_access_service.batch.pmtiles.generator import (
    PmtilesGenerationInProgressError,
    generate_pmtiles_for_parquets,
)
from data_access_service.utils.api_utils import api_key_auth
from data_access_service.utils.date_time_utils import time_it
from data_access_service.utils.routes_helper import get_api_instance
from data_access_service.utils.sse_utils import sse_it

router = APIRouter()


@router.put("/pmtiles/{uuid}/{key}", dependencies=[Depends(api_key_auth)])
@time_it
@sse_it
def create_pmtiles(request: Request, uuid: str, key: str):
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )
    try:
        return generate_pmtiles_for_parquets(api_instance, uuid, key)
    except PmtilesGenerationInProgressError as e:
        # Note: sse_it has already started a 200 stream, so this surfaces as an
        # SSE "error" event (same as the 503 above)
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,  # 409
            detail=str(e),
        )
