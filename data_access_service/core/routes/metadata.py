import logging
from http import HTTPStatus
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from data_access_service.core.data_index import has_original_netcdfs
from data_access_service.core.descriptor import Descriptor
from data_access_service.core.original_netcdf_index import ORIGINAL_NETCDF_KEY
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import get_api_instance

logger = logging.getLogger(__name__)

router = APIRouter()


async def _original_netcdf_metadata(uuid: str) -> Dict[str, Descriptor]:
    """The one key of the original NetCDF files of ``uuid``, from the data index.

    Empty when the uuid is not in the index or the index cannot be read, so
    the caller gets the same 404 as before this lookup existed.
    """
    try:
        # The DuckDB read is blocking; keep the event loop free.
        found = await run_in_threadpool(has_original_netcdfs, uuid)
    except Exception as exc:
        logger.error(
            "original NetCDF key lookup failed for uuid=%s: %s",
            uuid,
            exc,
            exc_info=True,
        )
        return {}
    if not found:
        return {}
    return {
        ORIGINAL_NETCDF_KEY: Descriptor(
            uuid=uuid, dname=ORIGINAL_NETCDF_KEY, cloud_optimised=False
        )
    }


@router.get("/metadata", dependencies=[Depends(api_key_auth)])
@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(
    uuid: Optional[str] = None,
    request: Request = None,
    include_original_netcdf: bool = Query(
        True,
        description="For a collection with no cloud-optimised data, return the "
        f"key of its original NetCDF files ({ORIGINAL_NETCDF_KEY}) instead of 404. "
        "Set to false to get cloud-optimised keys only",
    ),
):
    """The keys of a collection, each with ``cloud_optimised`` true or false.

    A collection with cloud-optimised data returns only those keys. One with
    none returns one key for its original NetCDF files, unless
    ``include_original_netcdf`` is false. es-indexer must set it to false: it
    reads every key here as cloud-optimised data.
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
        metadata = (
            await _original_netcdf_metadata(uuid) if include_original_netcdf else {}
        )
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
