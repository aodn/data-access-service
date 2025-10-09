import asyncio
import datetime
import json
import os
import re
import tempfile
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from os import error
from queue import Queue
from typing import Optional, List, Generator, AsyncGenerator

import dask.dataframe as dd
import numpy
import pandas
import pandas as pd
import psutil
import pytz
import xarray
import xarray as xr
from dask.dataframe import DataFrame
from dateutil import parser
from fastapi import HTTPException, Request, BackgroundTasks
from fastapi.responses import Response, FileResponse
from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry
from pydantic import BaseModel

from data_access_service import init_log
from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.core.api import gzip_compress
from data_access_service.core.constants import (
    COORDINATE_INDEX_PRECISION,
    DEPTH_INDEX_PRECISION,
    RECORD_PER_PARTITION,
    STR_TIME,
    STR_TIME_LOWER_CASE,
    STR_LONGITUDE_LOWER_CASE,
    STR_LATITUDE_LOWER_CASE,
    STR_DEPTH_LOWER_CASE,
)
from data_access_service.core.error import ErrorResponse
from data_access_service.models.value_count import ValueCount
from data_access_service.utils.date_time_utils import (
    DATE_FORMAT,
)

logger = init_log(Config.get_config())


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df: DataFrame) -> DataFrame:
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


def lazy_to_records_fast(df):
    for row in df.itertuples(index=False):
        yield row._asdict()  # Returns an OrderedDict; use dict(row._asdict()) if plain dict needed


# Use to remap the field name back to column that we pass it, the raw data itself may name the field differently
# for different dataset
def _generate_partial_json_array(
    filtered: dd.DataFrame, partition_size: int | None
) -> Generator[dict, None, None]:

    if filtered is None:
        return

    if partition_size is not None:
        ddf = filtered.repartition(npartitions=partition_size)
    else:
        ddf = filtered

    for partition in ddf.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())

        # Avoid NaN appear in the json output, map it to None and output
        # will become null for None value
        partition_df = partition_df.replace({numpy.nan: None})

        for record in lazy_to_records_fast(partition_df):
            filtered_record = {}
            # Time field is special, there is no standard name and appear diff in raw data,
            # here we unify it and call it time
            #
            # Use sys.intern to compress string usage to reduce memory
            if STR_TIME in record:
                filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(record[STR_TIME])
            elif STR_TIME_LOWER_CASE in record:
                filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(
                    record[STR_TIME_LOWER_CASE]
                )
            elif "JULD" in record:
                filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(record["JULD"])
            elif "timestamp" in record:
                filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(
                    datetime.fromtimestamp(
                        record["timestamp"], tz=timezone.utc
                    ).strftime(DATE_FORMAT)
                )
            elif "detection_timestamp" in record:
                if isinstance(record["detection_timestamp"], str):
                    filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(
                        parser.parse(record["detection_timestamp"])
                        .replace(tzinfo=timezone.utc)
                        .strftime(DATE_FORMAT)
                    )
                else:
                    filtered_record[STR_TIME_LOWER_CASE] = _reformat_date(
                        datetime.fromtimestamp(
                            record["detection_timestamp"], tz=timezone.utc
                        ).strftime(DATE_FORMAT)
                    )

            #  may need to add more field here
            if "LONGITUDE" in record:
                filtered_record[STR_LONGITUDE_LOWER_CASE] = (
                    round(record["LONGITUDE"], COORDINATE_INDEX_PRECISION)
                    if record["LONGITUDE"] is not None
                    else None
                )
            elif "longitude" in record:
                filtered_record[STR_LONGITUDE_LOWER_CASE] = (
                    round(record["longitude"], COORDINATE_INDEX_PRECISION)
                    if record["longitude"] is not None
                    else None
                )
            elif "lon" in record:
                filtered_record[STR_LONGITUDE_LOWER_CASE] = (
                    round(record["lon"], COORDINATE_INDEX_PRECISION)
                    if record["lon"] is not None
                    else None
                )

            if "LATITUDE" in record:
                filtered_record[STR_LATITUDE_LOWER_CASE] = (
                    round(record["LATITUDE"], COORDINATE_INDEX_PRECISION)
                    if record["LATITUDE"] is not None
                    else None
                )
            elif "latitude" in record:
                filtered_record[STR_LATITUDE_LOWER_CASE] = (
                    round(record["latitude"], COORDINATE_INDEX_PRECISION)
                    if record["latitude"] is not None
                    else None
                )
            elif "lat" in record:
                filtered_record[STR_LATITUDE_LOWER_CASE] = (
                    round(record["lat"], COORDINATE_INDEX_PRECISION)
                    if record["lat"] is not None
                    else None
                )

            if "DEPTH" in record:
                filtered_record[STR_DEPTH_LOWER_CASE] = (
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

    if req_date is not None and name == "end_date":
        # Require time and nanosecond precision (at least 7 digits after decimal)
        if not re.search(r"[T ]\d{2}:\d{2}:\d{2}\.\d{9,}", req_date):
            error_message = ErrorResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                details=f"Time with nanosecond precision missing in [{name}]. Example: 1970-02-03 12:34:56.123456789",
                parameters=f"{name}={req_date}",
            )
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST, detail=error_message.details
            )

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
def async_response_json(result: AsyncGenerator[dict, None], compress: bool):
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
                    if i is not None:
                        result_queue.put(i)  # Thread-safe append
                    else:
                        break
                result_queue.put(None)  # Sentinel to indicate completion

            loop.run_until_complete(collect())
        finally:
            loop.close()

    # Start thread to run async iteration
    thread = threading.Thread(target=run_async_iteration)
    thread.start()

    # Wait for results and collect
    while result is not None:
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


async def fetch_data(
    api_instance: API,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    start_depth: float | None,
    end_depth: float | None,
    columns: List[str],
) -> AsyncGenerator[dict | None, None]:
    try:
        result: Optional[dd.DataFrame | xr.Dataset] = api_instance.get_dataset(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            columns=columns,
        )
        # If we get nothing
        if result is None:
            # Indicate end of generator record
            yield None

    except Exception as e:
        # Indicate end of generator record
        yield None
    else:
        # Now we need to change the xarray if type match to 2D dataframe for processing
        if isinstance(result, xr.Dataset):
            # A way to get row count without compute and load all for xarray,
            # for xarray, the row count is the multiplication of all dimension
            # TODO: This is not enough, with multiple dimension, we can have a very
            # small value for first item, but the other dim can be big, so we
            # do not have a small enough partition
            count = None
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
            filtered, None if count is None else count // RECORD_PER_PARTITION + 1
        ):
            yield record


class HealthCheckResponse(BaseModel):
    status: str
    status_code: int


def get_api_instance(request: Request) -> API:
    instance = request.app.state.api_instance
    return instance


def calculate_cell_coordinates(lat_min, lat_max, lon_min, lon_max):
    precision = COORDINATE_INDEX_PRECISION
    dividing_lats = 10**precision


def get_memory_usage_percentage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    total_memory = psutil.virtual_memory().total
    logger.info("Total memory: %s", total_memory)
    return (memory_info.rss / total_memory) * 100


def round_coordinate_list(coordinate_list: List[float]) -> List[ValueCount]:
    """
    Round a list of coordinates to according to the precision and return a list of ValueCount objects.
    """
    precision = COORDINATE_INDEX_PRECISION
    rounded_list: List[ValueCount] = []

    for coordinate in coordinate_list:
        rounded_value = round(coordinate, precision)
        # Check if the rounded value already exists in the list
        existing_value = next(
            (item for item in rounded_list if item.value == rounded_value), None
        )

        if existing_value:
            existing_value.count += 1
        else:
            rounded_list.append(ValueCount(value=rounded_value, count=1))

    return rounded_list


def round_dates(date_list: List[pandas.Timestamp]):
    """
    Round a list of dates to format YYYY-mm
    """
    rounded_dates: List[ValueCount] = []
    for date in date_list:
        yyyy_mm_date = date.strftime("%Y-%m")
        # Check if the rounded value already exists in the list
        existing_value = next(
            (item for item in rounded_dates if item.value == yyyy_mm_date), None
        )

        if existing_value:
            existing_value.count += 1
        else:
            rounded_dates.append(ValueCount(value=yyyy_mm_date, count=1))

        if len(rounded_dates) > 1:
            raise error(
                "More than one date found, Only one month query is supported for now."
            )

    return rounded_dates


def generate_feature_collection(
    dataset: xarray.Dataset,
    lat_key: str,
    lon_key: str,
    time_key: str,
) -> FeatureCollection:
    """
    Generate a FeatureCollection from an xarray Dataset.
    This function extracts latitude, longitude, and time coordinates from the dataset,
    rounds the coordinates, and creates a FeatureCollection with Point geometries for each
    unique combination of latitude, longitude, and time.
    If a feature already exists for the same coordinates and time, an error is raised.
    dataset: xarray.Dataset
    lat_key: str - The key for latitude coordinates in the dataset. e.g. "lat", "LAT", "latitude" etc.
    lon_key: str - The key for longitude coordinates in the dataset. e.g. "lon", "LONGITUDE", "longitude" etc.
    time_key: str - The key for time coordinates in the dataset. e.g. "time", "TIME", "JUID" etc.
    """
    lats = dataset.coords[lat_key].values
    lons = dataset.coords[lon_key].values
    times = dataset.coords[time_key].values
    pandas_times = pandas.to_datetime(times)
    #
    rounded_lats = round_coordinate_list(lats)
    rounded_lons = round_coordinate_list(lons)
    rounded_times = round_dates(pandas_times)

    features = []
    for lon in rounded_lons:
        for lat in rounded_lats:
            for time in rounded_times:
                yyyy_mm_time = time.value
                geometry = {
                    "type": "Point",
                    "coordinates": [float(lon.value), float(lat.value)],
                }
                properties = {
                    "time": yyyy_mm_time,
                    "count": lon.count * lat.count * time.count,
                }
                feature = Feature(geometry=geometry, properties=properties)
                features.append(feature)

    return FeatureCollection(features=features)


def generate_rect_features(
    dataset: xarray.Dataset,
    lat_key: str,
    lon_key: str,
    time_key: str,
):
    """
    Generate a FeatureCollection  with rectangle features from an xarray Dataset.
    It is a shortterm solution for the zarr subsetting issue.
    """

    lats = dataset.coords[lat_key].values
    lons = dataset.coords[lon_key].values
    times = dataset.coords[time_key].values
    pandas_times = pandas.to_datetime(times)

    if len(pandas_times) == 0:
        logger.info("No data available in the dataset.")
        return None

    rounded_time = round_dates(pandas_times)[0]

    min_lat = float(numpy.nanmin(lats))
    max_lat = float(numpy.nanmax(lats))
    min_lon = float(numpy.nanmin(lons))
    max_lon = float(numpy.nanmax(lons))

    rect_polygon = [
        [min_lon, min_lat],
        [min_lon, max_lat],
        [max_lon, max_lat],
        [max_lon, min_lat],
        [min_lon, min_lat],
    ]

    geometry = Geometry(type="Polygon", coordinates=[rect_polygon])
    feature = Feature(
        geometry=geometry,
        properties={
            "date": rounded_time.value,
            "count": len(lats) * len(lons) * rounded_time.count,
        },
    )
    return [feature]
