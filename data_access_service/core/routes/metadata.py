from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from data_access_service.utils.api_utils import api_key_auth
from data_access_service.utils.routes_helper import get_api_instance

router = APIRouter()


@router.get("/metadata", dependencies=[Depends(api_key_auth)])
@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(uuid: Optional[str] = None, request: Request = None):
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )

    metadata = api_instance.get_mapped_meta_data(uuid)

    if metadata.get("not_exist") is not None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Dataset not found.",
        )

    return metadata


@router.get("/metadata/{uuid}/raw", dependencies=[Depends(api_key_auth)])
async def get_raw_metadata(uuid: str, request: Request):
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )
    return api_instance.get_raw_meta_data(uuid)
