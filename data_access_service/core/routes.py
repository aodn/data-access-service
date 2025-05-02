from data_access_service import init_log
from data_access_service.core.api import API
from data_access_service.utils.api_utils import api_key_auth
from data_access_service.core.api import gzip_compress
from data_access_service.core.constants import (
    COORDINATE_INDEX_PRECISION,
    DEPTH_INDEX_PRECISION,
)
from data_access_service.core.error import ErrorResponse
from data_access_service.config.config import Config

import logging
from typing import Optional, List
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, Query, BackgroundTasks
from fastapi.responses import Response, FileResponse
from pydantic import BaseModel
import dataclasses
import datetime
import json
import os
import tempfile

import dask.dataframe
import psutil
import xarray as xr
import numpy
import pandas as pd
import dask.dataframe as dd
from dateutil import parser
from http import HTTPStatus

router = APIRouter(prefix=Config.BASE_URL)

RECORD_PER_PARTITION: Optional[int] = 1000
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"

logger = init_log(Config.get_config())

# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json_array(dask_instance, compress: bool = False):
    record_list = []
    for partition in dask_instance.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient="records"):
            record_list.append(record)

    json_array = json.dumps(record_list)

    if compress:
        return gzip_compress(json_array)
    else:
        return json_array


def _generate_partial_json_array(d: dask.dataframe.DataFrame, compress: bool = False):
    record_list = []
    for partition in d.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient="records"):
            filtered_record = {}
            # Time field is special, there is no stand name and appear diff in raw data,
            # here we unify it and call it time
            if "TIME" in record:
                filtered_record["time"] = _reformat_date(record["TIME"])
            elif "JULD" in record:
                filtered_record["time"] = _reformat_date(record["JULD"])
            elif "timestamp" in record:
                filtered_record["time"] = _reformat_date(
                    datetime.datetime.fromtimestamp(
                        record["timestamp"], tz=timezone.utc
                    ).strftime(DATE_FORMAT)
                )

            #  may need to add more field here
            if "LONGITUDE" in record:
                filtered_record["longitude"] = round(
                    record["LONGITUDE"], COORDINATE_INDEX_PRECISION
                )
            if "LATITUDE" in record:
                filtered_record["latitude"] = round(
                    record["LATITUDE"], COORDINATE_INDEX_PRECISION
                )
            if "DEPTH" in record:
                filtered_record["depth"] = round(record["DEPTH"], DEPTH_INDEX_PRECISION)
            record_list.append(filtered_record)

    json_array = json.dumps(record_list)

    if compress:
        return gzip_compress(json_array)
    else:
        return json_array


# currently only want year, month and date.
def _reformat_date(date: str):
    parsed_date = parser.isoparse(date)
    formatted_date = parsed_date.strftime("%Y-%m-%d")
    return formatted_date


def _round_5_decimal(value: float) -> float:
    # as they are only used for the frontend map display, so we don't need to have too many decimals
    return round(value, 5)


def _verify_datatime_param(name: str, req_date: str) -> datetime:
    _date = None

    try:
        if req_date is not None:
            _date = parser.isoparse(req_date)
            if _date.tzinfo is None:
                _date = _date.replace(tzinfo=timezone.utc)

    except (ValueError, TypeError) as e:
        error_message = ErrorResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            details=f"Incorrect format [{name}]",
            parameters=f"{name}={req_date}",
        )

        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail=error_message.details
        )

    return _date


def _verify_depth_param(name: str, req_value: numpy.double | None) -> numpy.double | None:
    if req_value is not None and req_value > 0.0:
        error_message = ErrorResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            details=f"Depth cannot greater than zero",
            parameters=f"{name}={req_value}",
        )

        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail=error_message.details
        )
    else:
        return req_value


def _verify_to_index_flag_param(flag: str | None) -> bool:
    if (flag is not None) and (flag.lower() == "true"):
        return True
    else:
        return False


def _response_json(filtered: pd.DataFrame, compress: bool):
    ddf: dask.dataframe.DataFrame = dd.from_pandas(
        filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1
    )
    response = Response(
        _generate_json_array(ddf, compress), media_type="application/json"
    )

    if compress:
        response.headers["Content-Encoding"] = "gzip"

    return response


def _response_partial_json(filtered: pd.DataFrame, compress: bool):
    ddf: dask.dataframe.DataFrame = dd.from_pandas(
        filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1
    )
    response = Response(
        _generate_partial_json_array(ddf, compress), media_type="application/json"
    )

    if compress:
        response.headers["Content-Encoding"] = "gzip"

    return response


# TODO: Need to use the metadata to assign correct type to netcdf, right now field type is wrong
def _response_netcdf(filtered: pd.DataFrame, background_tasks: BackgroundTasks):
    # Convert the DataFrame to an xarray Dataset
    ds = xr.Dataset.from_dataframe(filtered)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp_file:
        tmp_file_name = tmp_file.name
        ds.to_netcdf(tmp_file_name)

    # Send the file as a response to the client
    response = FileResponse(
        path=tmp_file_name,
        filename="output.nc",
        media_type="application/x-netcdf",
    )

    # Clean up the temporary file after sending the response
    def _remove_file_if_exists(file_path):
        """Helper function to remove file if it exists"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error removing file {file_path}: {e}")

    background_tasks.add_task(lambda: _remove_file_if_exists(tmp_file_name))
    return response


class HealthCheckResponse(BaseModel):
    status: str
    status_code: int


def get_api_instance(request: Request) -> API:
    instance = request.app.state.api_instance
    return instance


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(request: Request):
    """
    Health check endpoint.
    """
    api_instance = get_api_instance(request)
    if api_instance.get_api_status():
        return HealthCheckResponse(status="UP", status_code=HTTPStatus.OK)
    else:
        return HealthCheckResponse(
            status="STARTING", status_code=HTTPStatus.SERVICE_UNAVAILABLE
        )


@router.get("/metadata", dependencies=[Depends(api_key_auth)])
@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(uuid: Optional[str] = None, request: Request = None):
    api_instance = get_api_instance(request)
    if uuid is not None:
        return dataclasses.asdict(api_instance.get_mapped_meta_data(uuid))
    else:
        return list(api_instance.get_mapped_meta_data(None))


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


@router.get("/data/{uuid}/has_data", dependencies=[Depends(api_key_auth)])
async def has_data(
    uuid: str,
    request: Request,
    start_date: Optional[str] = MIN_DATE,
    end_date: Optional[str] = datetime.datetime.now(datetime.timezone.utc).strftime(
        DATE_FORMAT
    ),
):
    api_instance = get_api_instance(request)
    logger.info(
        "Request details: %s", json.dumps(dict(request.query_params.multi_items()))
    )
    start_date = _verify_datatime_param("start_date", start_date)
    end_date = _verify_datatime_param("end_date", end_date)
    result = str(api_instance.has_data(uuid, start_date, end_date)).lower()
    return Response(result, media_type="application/json")


@router.get("/data/{uuid}/temporal_extent", dependencies=[Depends(api_key_auth)])
async def get_temporal_extent(uuid: str, request: Request):
    api_instance = get_api_instance(request)
    try:
        start_date, end_date = api_instance.get_temporal_extent(uuid)
        result = [
            {
                "start_date": start_date.strftime(DATE_FORMAT),
                "end_date": end_date.strftime(DATE_FORMAT),
            }
        ]
        return Response(content=json.dumps(result), media_type="application/json")
    except ValueError:
        raise HTTPException(status_code=404, detail="Temporal extent not found")


@router.get("/data/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_data(
    uuid: str,
    request: Request,
    background_tasks: BackgroundTasks,
    start_date: Optional[str] = Query(default=MIN_DATE),
    end_date: Optional[str] = Query(
        default=datetime.datetime.now(timezone.utc).strftime(DATE_FORMAT)
    ),
    columns: Optional[List[str]] = Query(default=None),
    start_depth: Optional[float] = Query(default=-1.0),
    end_depth: Optional[float] = Query(default=-1.0),
    is_to_index: Optional[bool] = Query(default=None),
    f: Optional[str] = Query(default="json"),
):
    api_instance = get_api_instance(request)
    logger.info(
        "Request details: %s", json.dumps(dict(request.query_params.multi_items()))
    )
    start_date = _verify_datatime_param("start_date", start_date)
    end_date = _verify_datatime_param("end_date", end_date)

    result: Optional[pd.DataFrame] = api_instance.get_dataset_data(
        uuid=uuid, date_start=start_date, date_end=end_date, columns=columns
    )

    # if result is None, return empty response
    if result is None:
        return Response("[]", media_type="application/json")

    start_depth = _verify_depth_param("start_depth", start_depth)
    end_depth = _verify_depth_param("end_depth", end_depth)

    is_to_index = _verify_to_index_flag_param(is_to_index)

    # The cloud optimized format is fast to lookup if there is an index, some field isn't part of the
    # index and therefore will not gain to filter by those field, indexed fields are site_code, timestamp, polygon

    # Depth is below sea level zero, so logic slightly diff
    if start_depth > 0 and end_depth > 0:
        filtered = result[
            (result["DEPTH"] <= start_depth) & (result["DEPTH"] >= end_depth)
        ]
    elif start_depth > 0:
        filtered = result[(result["DEPTH"] <= start_depth)]
    elif end_depth > 0:
        filtered = result[result["DEPTH"] >= end_depth]
    else:
        filtered = result

    logger.info("Record number return %s for query", len(filtered.index))
    logger.info("Memory usage: %s", get_memory_usage_percentage())

    if f == "json":
        # Depends on whether receiver support gzip encoding
        compress = "gzip" in request.headers.get("Accept-Encoding", "")
        if is_to_index:
            return _response_partial_json(filtered, compress)
        else:
            return _response_json(filtered, compress)
    elif f == "netcdf":
        return _response_netcdf(filtered, background_tasks)
    return None


def get_memory_usage_percentage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    total_memory = psutil.virtual_memory().total
    logger.info("Total memory: %s", total_memory)
    return (memory_info.rss / total_memory) * 100
