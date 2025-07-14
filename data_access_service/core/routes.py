import asyncio
import threading
import datetime
import json
import os
import tempfile

import psutil
import pytz
import xarray as xr
import numpy
import pandas as pd
import dask.dataframe as dd

from queue import Queue
from functools import reduce
from operator import mul
from dask.dataframe import DataFrame

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

from typing import Optional, List, Generator, AsyncGenerator
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, Query, BackgroundTasks
from fastapi.responses import Response, FileResponse
from pydantic import BaseModel
from dateutil import parser
from http import HTTPStatus

from data_access_service.utils.date_time_utils import (
    ensure_timezone,
    MIN_DATE,
    DATE_FORMAT,
)
from data_access_service.utils.sse_wrapper import sse_wrapper

RECORD_PER_PARTITION: Optional[int] = 500

router = APIRouter(prefix=Config.BASE_URL)
logger = init_log(Config.get_config())


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df: DataFrame) -> DataFrame:
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json_array(dask_instance, compress: bool = False):

    async def get_records():
        for partition in dask_instance.to_delayed():
            partition_df = convert_non_numeric_to_str(partition.compute())
            for record in partition_df.to_dict(orient="records"):
                yield record

    return _async_response_json(get_records(), compress)


# Use to remap the field name back to column that we pass it, the raw data itself may name the field differently
# for different dataset
def _generate_partial_json_array(
    filtered: dd.DataFrame, partition_size: int
) -> Generator[dict, None, None]:

    ddf = filtered.repartition(npartitions=partition_size)

    for partition in ddf.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())

        # Avoid NaN appear in the json output, map it to None and output
        # will become null for None value
        partition_df = partition_df.replace({numpy.nan: None})

        for record in partition_df.to_dict(orient="records"):
            filtered_record = {}
            # Time field is special, there is no standard name and appear diff in raw data,
            # here we unify it and call it time
            if "TIME" in record:
                filtered_record["time"] = _reformat_date(record["TIME"])
            elif "time" in record:
                filtered_record["time"] = _reformat_date(record["time"])
            elif "JULD" in record:
                filtered_record["time"] = _reformat_date(record["JULD"])
            elif "timestamp" in record:
                filtered_record["time"] = _reformat_date(
                    datetime.fromtimestamp(
                        record["timestamp"], tz=timezone.utc
                    ).strftime(DATE_FORMAT)
                )
            elif "detection_timestamp" in record:
                filtered_record["time"] = _reformat_date(
                    datetime.fromtimestamp(
                        record["detection_timestamp"], tz=timezone.utc
                    ).strftime(DATE_FORMAT)
                )

            #  may need to add more field here
            if "LONGITUDE" in record:
                filtered_record["longitude"] = (
                    round(record["LONGITUDE"], COORDINATE_INDEX_PRECISION)
                    if record["LONGITUDE"] is not None
                    else None
                )
            elif "longitude" in record:
                filtered_record["longitude"] = (
                    round(record["longitude"], COORDINATE_INDEX_PRECISION)
                    if record["longitude"] is not None
                    else None
                )
            elif "lon" in record:
                filtered_record["longitude"] = (
                    round(record["lon"], COORDINATE_INDEX_PRECISION)
                    if record["lon"] is not None
                    else None
                )

            if "LATITUDE" in record:
                filtered_record["latitude"] = (
                    round(record["LATITUDE"], COORDINATE_INDEX_PRECISION)
                    if record["LATITUDE"] is not None
                    else None
                )
            elif "latitude" in record:
                filtered_record["latitude"] = (
                    round(record["latitude"], COORDINATE_INDEX_PRECISION)
                    if record["latitude"] is not None
                    else None
                )
            elif "lat" in record:
                filtered_record["latitude"] = (
                    round(record["lat"], COORDINATE_INDEX_PRECISION)
                    if record["lat"] is not None
                    else None
                )

            if "DEPTH" in record:
                filtered_record["depth"] = (
                    round(record["DEPTH"], DEPTH_INDEX_PRECISION)
                    if record["DEPTH"] is not None
                    else None
                )
            yield filtered_record


# currently only want year, month and date.
def _reformat_date(date: str):
    parsed_date = parser.isoparse(date)
    formatted_date = parsed_date.strftime("%Y-%m-%d")
    return formatted_date


def _round_5_decimal(value: float) -> float:
    # as they are only used for the frontend map display, so we don't need to have too many decimals
    return round(value, 5)


def _verify_datatime_param(name: str, req_date: str) -> pd.Timestamp:
    _date = None

    try:
        if req_date is not None:
            _date = pd.Timestamp(req_date)
            if _date.tz is None:
                _date = _date.tz_localize(pytz.UTC)

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


def _verify_depth_param(
    name: str, req_value: numpy.double | None
) -> numpy.double | None:
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


def _verify_to_index_flag_param(flag: str | bool | None) -> bool:
    if (flag is not None) and bool(flag):
        return True
    else:
        return False


# Parallel process records and map the field back to standard name
def _async_response_json(result: AsyncGenerator[dict, None], compress: bool):
    # Thread-safe queue to collect results
    result_queue = Queue()
    json_results = []

    def run_async_iteration():
        # Create a new event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:

            async def collect():
                async for i in result:
                    result_queue.put(i)  # Thread-safe append
                result_queue.put(None)  # Sentinel to indicate completion

            loop.run_until_complete(collect())
        finally:
            loop.close()

    # Start thread to run async iteration
    thread = threading.Thread(target=run_async_iteration)
    thread.start()

    # Wait for results and collect
    while True:
        item = result_queue.get()
        if item is None:  # Sentinel indicates completion
            break
        json_results.append(item)  # Serialize each item
    thread.join()  # Ensure thread completes

    if compress:
        response = Response(
            gzip_compress(json.dumps(json_results)), media_type="application/json"
        )
        response.headers["Content-Encoding"] = "gzip"
        return response
    else:
        return Response(json.dumps(json_results), media_type="application/json")


def _response_json(result: Generator[dict, None, None], compress: bool):
    json_array = json.dumps([x for x in result])

    if compress:
        response = Response(gzip_compress(json_array), media_type="application/json")
        response.headers["Content-Encoding"] = "gzip"
        return response
    else:
        return Response(json_array, media_type="application/json")


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
            logger.error(f"Error removing file {file_path}: {e}")

    background_tasks.add_task(lambda: _remove_file_if_exists(tmp_file_name))
    return response


async def _fetch_data(
    api_instance: API,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    start_depth: float | None,
    end_depth: float | None,
    columns: List[str],
) -> AsyncGenerator[dict, None]:
    try:
        result: Optional[dd.DataFrame | xr.Dataset] = api_instance.get_dataset_data(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            columns=columns,
        )
        # If we get nothing
        if result is None:
            return

    except ValueError as e:
        # TODO If error return empty response. This maybe hard to debug
        return
    else:
        # Now we need to change the xarray if type match to 2D dataframe for processing
        if isinstance(result, xr.Dataset):
            # A way to get row count without compute and load all for xarray,
            # for xarray, the row count is the multiplication of all dimension
            count = reduce(mul, [result.sizes[v] for v in list(result.sizes.keys())])
            result = api_instance.zarr_to_dask_dataframe(
                result,
                api_instance.map_column_names(
                    uuid, key, api_instance.map_column_names(uuid, key, columns)
                ),
            )
        else:
            count = len(result.index)

        start_depth = _verify_depth_param("start_depth", start_depth)
        end_depth = _verify_depth_param("end_depth", end_depth)

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

        logger.info("Memory usage: %s", get_memory_usage_percentage())

        for record in _generate_partial_json_array(
            filtered, count // RECORD_PER_PARTITION + 1
        ):
            yield record


class HealthCheckResponse(BaseModel):
    status: str
    status_code: int


def get_api_instance(request: Request) -> API:
    instance = request.app.state.api_instance
    return instance


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


def get_memory_usage_percentage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    total_memory = psutil.virtual_memory().total
    logger.info("Total memory: %s", total_memory)
    return (memory_info.rss / total_memory) * 100
