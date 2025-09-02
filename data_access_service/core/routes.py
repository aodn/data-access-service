import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Optional, List

from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import Response
from xarray import Dataset

from data_access_service import init_log
from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.utils.api_utils import api_key_auth
from data_access_service.utils.date_time_utils import (
    ensure_timezone,
    MIN_DATE,
    DATE_FORMAT,
)
from data_access_service.utils.routes_helper import (
    HealthCheckResponse,
    get_api_instance,
    _verify_datatime_param,
    _fetch_data,
    _async_response_json,
    generate_feature_collection,
    generate_rect_feature_collection,
)
from data_access_service.utils.sse_wrapper import sse_wrapper

router = APIRouter(prefix=Config.BASE_URL)
logger = init_log(Config.get_config())


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(request: Request):
    """
    Health check endpoint. The init now become very slow due to the need to load zarr data on init
    so we report status code OK, to avoid AWS timeout but the status value is STARTING
    """
    api_instance = get_api_instance(request)
    if api_instance.get_api_status():
        return HealthCheckResponse(status="UP", status_code=HTTPStatus.OK)
    else:
        return HealthCheckResponse(status="STARTING", status_code=HTTPStatus.OK)


@router.get("/metadata", dependencies=[Depends(api_key_auth)])
@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(uuid: Optional[str] = None, request: Request = None):
    api_instance = get_api_instance(request)
    return api_instance.get_mapped_meta_data(uuid)


@router.get("/metadata/{uuid}/raw", dependencies=[Depends(api_key_auth)])
async def get_raw_metadata(uuid: str, request: Request):
    api_instance = get_api_instance(request)
    return api_instance.get_raw_meta_data(uuid)


@router.get("/data/{uuid}/notebook_url", dependencies=[Depends(api_key_auth)])
async def get_notebook_url(uuid: str, request: Request):
    i = API.get_notebook_from(uuid)
    if isinstance(i, ValueError):
        raise HTTPException(status_code=404, detail="Notebook URL not found")
    return i


@router.get("/data/{uuid}/{key}/has_data", dependencies=[Depends(api_key_auth)])
async def has_data(
    uuid: str,
    key: str,
    request: Request,
    start_date: Optional[str] = MIN_DATE,
    end_date: Optional[str] = datetime.now(timezone.utc).strftime(DATE_FORMAT),
):
    api_instance = get_api_instance(request)
    logger.info(
        "Request details: %s", json.dumps(dict(request.query_params.multi_items()))
    )
    start_date = _verify_datatime_param("start_date", start_date)
    end_date = _verify_datatime_param("end_date", end_date)
    result = str(api_instance.has_data(uuid, key, start_date, end_date)).lower()
    return Response(result, media_type="application/json")


@router.get("/data/{uuid}/{key}/temporal_extent", dependencies=[Depends(api_key_auth)])
async def get_temporal_extent(uuid: str, key: str, request: Request):
    api_instance = get_api_instance(request)
    try:
        start_date, end_date = api_instance.get_temporal_extent(uuid, key)
        result = [
            {
                "start_date": ensure_timezone(start_date).strftime(DATE_FORMAT),
                "end_date": ensure_timezone(end_date).strftime(DATE_FORMAT),
            }
        ]
        return Response(content=json.dumps(result), media_type="application/json")
    except ValueError:
        raise HTTPException(status_code=404, detail="Temporal extent not found")


@router.get("/data/{uuid}/{key}/indexing_values", dependencies=[Depends(api_key_auth)])
async def get_indexing_values(
    request: Request, uuid: str, key: str, start_date: str, end_date: str
):
    """
    Get feature collection for a Zarr dataset with the given UUID and key.
    This endpoint is an investigation endpoint. Will try to use it later it necessary, Not in use right now.
    """
    # if any parameter is not provided, is a bad request
    if not all([uuid, key, start_date, end_date]):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Missing required parameters",
        )
    # the param "key" should contains the extension of the file, .parquet / .zarr.
    if not key.endswith((".parquet", ".zarr")):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Invalid file format. Key must end with .parquet or .zarr",
        )

    # parquet might support in the future, but right now we only support zarr
    if not key.endswith(".zarr"):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="This endpoint only supports zarr data format",
        )

    api = get_api_instance(request)
    data_source = api.get_dataset_data(
        uuid=uuid,
        key=key,
        date_start=_verify_datatime_param("start_date", start_date),
        date_end=_verify_datatime_param("end_date", end_date),
    )
    if data_source is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"No data found with provided params for dataset {uuid} with key {key}",
        )

    if not isinstance(data_source, Dataset):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Dataset {uuid} with key {key} is not a Zarr dataset. Please doublecheck or contact AODN",
        )

    lat_key = api.map_column_names(uuid=uuid, key=key, columns=["LATITUDE"])[0]
    lon_key = api.map_column_names(uuid=uuid, key=key, columns=["LONGITUDE"])[0]
    time_key = api.map_column_names(uuid=uuid, key=key, columns=["TIME"])[0]

    if (
        lat_key not in data_source.coords
        or lon_key not in data_source.coords
        or time_key not in data_source.coords
    ):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Dataset {uuid} with key {key} does not contain required coordinates: {lat_key}, {lon_key}, {time_key}",
        )

    feature_collection = generate_feature_collection(
        dataset=data_source, lat_key=lat_key, lon_key=lon_key, time_key=time_key
    )
    return Response(
        content=json.dumps(feature_collection.to_dict()), media_type="application/json"
    )


@router.get("/data/{uuid}/{key}/zarr_rect", dependencies=[Depends(api_key_auth)])
async def get_zarr_rectangles(
    request: Request, uuid: str, key: str, start_date: str, end_date: str
):
    if not all([uuid, key, start_date, end_date]):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Missing required parameters",
        )
    if not key.endswith(".zarr"):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="This endpoint only supports zarr data format",
        )

    api = get_api_instance(request)
    data_source = api.get_dataset_data(
        uuid=uuid,
        key=key,
        date_start=_verify_datatime_param("start_date", start_date),
        date_end=_verify_datatime_param("end_date", end_date),
    )

    if data_source is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"No data found with provided params for dataset {uuid} with key {key}",
        )
    if not isinstance(data_source, Dataset):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Dataset {uuid} with key {key} is not a Zarr dataset. Please doublecheck or contact AODN",
        )

    lat_key = api.map_column_names(uuid=uuid, key=key, columns=["LATITUDE"])[0]
    lon_key = api.map_column_names(uuid=uuid, key=key, columns=["LONGITUDE"])[0]
    time_key = api.map_column_names(uuid=uuid, key=key, columns=["TIME"])[0]

    if (
        lat_key not in data_source.coords
        or lon_key not in data_source.coords
        or time_key not in data_source.coords
    ):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Dataset {uuid} with key {key} does not contain required coordinates: {lat_key}, {lon_key}, {time_key}",
        )

    rect_feature_collection = generate_rect_feature_collection(
        dataset=data_source, lat_key=lat_key, lon_key=lon_key, time_key=time_key
    )
    return Response(
        content=json.dumps(rect_feature_collection), media_type="application/json"
    )


@router.get("/data/{uuid}/{key}", dependencies=[Depends(api_key_auth)])
async def get_data(
    request: Request,
    uuid: str,
    key: str,
    start_date: Optional[str] = Query(default=MIN_DATE),
    end_date: Optional[str] = Query(
        default=datetime.now(timezone.utc).strftime(DATE_FORMAT)
    ),
    columns: Optional[List[str]] = Query(default=None),
    start_depth: Optional[float] = Query(default=-1.0),
    end_depth: Optional[float] = Query(default=-1.0),
    f: Optional[str] = Query(default="json"),
):
    api_instance = get_api_instance(request)
    logger.info(
        """
        Request details:
            uuid=%s,
            columns=%s,
            start_date=%s,
            end_date=%s,
            start_depth=%s,
            end_depth=%s
        """,
        uuid,
        columns,
        start_date,
        end_date,
        start_depth,
        end_depth,
    )
    start_date = _verify_datatime_param("start_date", start_date)
    end_date = _verify_datatime_param("end_date", end_date)

    sse = f.startswith("sse/")
    compress = "gzip" in request.headers.get("Accept-Encoding", "")

    if sse:
        return await sse_wrapper(
            _fetch_data,
            api_instance,
            uuid,
            key,
            start_date,
            end_date,
            start_depth,
            end_depth,
            columns,
        )
    else:
        result = _fetch_data(
            api_instance,
            uuid,
            key,
            start_date,
            end_date,
            start_depth,
            end_depth,
            columns,
        )

        if f == "json":
            # Depends on whether receiver support gzip encoding
            logger.info("Use compressed output %s", compress)
            return _async_response_json(result, compress)
        # elif f == "netcdf":
        #    return _response_netcdf(filtered, background_tasks)
        return None
