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

    Empty when the uuid has none.
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
        description="Also return the key of the collection's data-index files "
        f"({ORIGINAL_NETCDF_KEY}), when it has any. "
        "Set to false to get cloud-optimised keys only",
    ),
):
    """The keys of a collection, each with a ``type``.

    Returns the cloud-optimised keys (``cloud_optimised``) and, when the
    collection has data-index files, one more key for them (``data_index``).
    The caller picks by ``type``. ``include_data_index=false`` leaves the
    data-index key out. 404 when the collection has no key at all.

    Without a uuid: the cloud-optimised keys of every collection.
    """
    api_instance = get_api_instance(request)
    # Check API initialization status first
    if not api_instance.get_api_status():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail="API is not ready. Metadata initialization is still in progress.",
        )

    metadata = api_instance.get_mapped_meta_data(uuid)
    if uuid is None:
        return metadata

    has_co_keys = metadata.get("not_exist") is None
    co_keys = metadata if has_co_keys else {}
    data_index_key = _data_index_metadata(uuid) if include_data_index else {}

    if not has_co_keys and not data_index_key:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Dataset not found.",
        )

    # A new dict: co_keys is the cached catalogue entry, it must not change.
    return {**co_keys, **data_index_key}


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
