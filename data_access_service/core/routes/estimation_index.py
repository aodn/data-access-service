from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request

from data_access_service.batch.estimation.generator import (
    EstimationIndexGenerationInProgressError,
    generate_estimation_index_for_parquets,
)
from data_access_service.core.estimation_index import clear_sidecar_cache
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import get_api_instance
from data_access_service.utils.date_time_utils import time_it
from data_access_service.utils.sse_utils import sse_it

router = APIRouter()


@router.put("/estimation-index/{uuid}/{key}", dependencies=[Depends(api_key_auth)])
@time_it
@sse_it
def create_estimation_index(request: Request, uuid: str, key: str):
    """Build the estimate index for one parquet key and upload it.

    The same work the weekly batch does, for one dataset, so an environment can
    be backfilled or a dataset rebuilt without re-running pmtiles.
    """
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )
    try:
        built = generate_estimation_index_for_parquets(api_instance, uuid, key)
    except EstimationIndexGenerationInProgressError as e:
        # Note: sse_it has already started a 200 stream, so this surfaces as an
        # SSE "error" event (same as the 503 above)
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,  # 409
            detail=str(e),
        )
    # The old sidecar is cached for a few minutes; drop it so the next estimate
    # sees what we just uploaded.
    clear_sidecar_cache()
    return built
