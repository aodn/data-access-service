from http import HTTPStatus
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from data_access_service.core.data_index import is_data_index_uuid
from data_access_service.core.descriptor import Descriptor, KeyType
from data_access_service.core.data_index_files import ORIGINAL_NETCDF_KEY
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import get_api_instance

router = APIRouter()


def _data_index_metadata(uuid: str) -> Dict[str, Descriptor]:
    """The one key of the data-index files of ``uuid``.

    Empty when the uuid has none, so the caller gets the same 404 as before.
    A lookup in memory, so it answers as fast as the cloud-optimised keys.
    """
    if not is_data_index_uuid(uuid):
        return {}
    return {
        ORIGINAL_NETCDF_KEY: Descriptor(
            uuid=uuid, dname=ORIGINAL_NETCDF_KEY, type=KeyType.DATA_INDEX
        )
    }


@router.get("/metadata", dependencies=[Depends(api_key_auth)])
@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(
    uuid: Optional[str] = None,
    request: Request = None,
    include_data_index: bool = Query(
        True,
        description="For a collection with no cloud-optimised data, return the "
        f"key of its data-index files ({ORIGINAL_NETCDF_KEY}) instead of 404. "
        "Set to false to get cloud-optimised keys only",
    ),
):
    """The keys of a collection, each with a ``type``.

    A collection with cloud-optimised data returns only those keys
    (``cloud_optimised``). One with none returns one key for its data-index
    files (``data_index``), unless ``include_data_index`` is false.
    """
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )

    metadata = api_instance.get_mapped_meta_data(uuid)

    if metadata.get("not_exist") is not None:
        metadata = _data_index_metadata(uuid) if include_data_index else {}
        if not metadata:
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
