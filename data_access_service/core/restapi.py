import dataclasses
import datetime
from datetime import timezone
import json
import logging
import os
import tempfile

import dask.dataframe
import psutil
import xarray as xr
import numpy
import pandas as pd
import dask.dataframe as dd

from typing import Optional
from flask import Blueprint, request, abort, Response, send_file
from dateutil import parser
from http import HTTPStatus

from pandas import DataFrame

from data_access_service import app
from data_access_service.core.api import gzip_compress
from data_access_service.core.constants import (
    COORDINATE_INDEX_PRECISION,
    DEPTH_INDEX_PRECISION,
)
from data_access_service.core.error import ErrorResponse

restapi = Blueprint("restapi", __name__)
log = logging.getLogger(__name__)

RECORD_PER_PARTITION: Optional[int] = 1000
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json_array(dask, compress: bool = False):
    record_list = []
    for partition in dask.to_delayed():
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

    except (ValueError, TypeError) as e:
        error_message = ErrorResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            details=f"Incorrect format [{name}]",
            parameters=f"{name}={req_date}",
        )

        abort(HTTPStatus.BAD_REQUEST, error_message)

    return _date


def _verify_depth_param(name: str, req_value: numpy.double) -> numpy.double | None:
    if req_value is not None and req_value > 0.0:
        error_message = ErrorResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            details=f"Depth cannot greater than zero",
            parameters=f"{name}={req_value}",
        )

        abort(HTTPStatus.BAD_REQUEST, error_message)
    else:
        return req_value


def _verify_to_index_flag_param(flag: str) -> bool:
    if (flag is not None) and (flag.lower() == "true"):
        return True
    else:
        return False


def _response_json(filtered: DataFrame, compress: bool):
    ddf: dask.dataframe.DataFrame = dd.from_pandas(
        filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1
    )
    response = Response(
        _generate_json_array(ddf, compress), mimetype="application/json"
    )

    if compress:
        response.headers["Content-Encoding"] = "gzip"

    return response


def _response_partial_json(filtered: DataFrame, compress: bool):
    ddf: dask.dataframe.DataFrame = dd.from_pandas(
        filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1
    )
    response = Response(
        _generate_partial_json_array(ddf, compress), mimetype="application/json"
    )

    if compress:
        response.headers["Content-Encoding"] = "gzip"

    return response


# TODO: Need to use the metadata to assign correct type to netcdf, right now field type is wrong
def _response_netcdf(filtered: DataFrame):
    # Convert the DataFrame to an xarray Dataset
    ds = xr.Dataset.from_dataframe(filtered)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp_file:
        tmp_file_name = tmp_file.name
        ds.to_netcdf(tmp_file_name)

    # Send the file as a response to the client
    response = send_file(
        tmp_file_name,
        as_attachment=True,
        download_name="output.nc",
        mimetype="application/x-netcdf",
    )

    # Clean up the temporary file after sending the response
    def _remove_file_if_exists(file_path):
        """Helper function to remove file if it exists"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error removing file {file_path}: {e}")

    response.call_on_close(lambda: _remove_file_if_exists(tmp_file_name))

    return response


@restapi.route("/health", methods=["GET"])
def health_check() -> Response:
    return Response("healthy", mimetype="application/json")


@restapi.route("/metadata", methods=["GET"])
@restapi.route("/metadata/<string:uuid>", methods=["GET"])
def get_mapped_metadata(uuid=None):
    if uuid is not None:
        return dataclasses.asdict(app.api.get_mapped_meta_data(uuid))
    else:
        return list(app.api.get_mapped_meta_data(None))


@restapi.route("/metadata/<string:uuid>/raw", methods=["GET"])
def get_raw_metadata(uuid: str):
    return app.api.get_raw_meta_data(uuid)


@restapi.route("/data/<string:uuid>/notebook_url", methods=["GET"])
def get_notebook_url(uuid: str):
    i = app.api.get_notebook_from(uuid)
    if isinstance(i, ValueError):
        abort(404)
    else:
        return i


@restapi.route("/data/<string:uuid>/has_data", methods=["GET"])
def has_data(uuid):
    start_date = _verify_datatime_param(
        "start_date", request.args.get("start_date", default=MIN_DATE)
    )
    end_date = _verify_datatime_param(
        "end_date",
        request.args.get(
            "end_date",
            default=datetime.datetime.now(datetime.timezone.utc).strftime(DATE_FORMAT),
        ),
    )
    result = str(app.api.has_data(uuid, start_date, end_date)).lower()
    return Response(result, mimetype="application/json")


@restapi.route("/data/<string:uuid>/temporal_extent", methods=["GET"])
def get_temporal_extent(uuid):
    temp: (datetime, datetime) = app.api.get_temporal_extent(uuid)
    result = [
        {
            "start_date": temp[0].strftime(DATE_FORMAT),
            "end_date": temp[1].strftime(DATE_FORMAT),
        }
    ]
    return Response(json.dumps(result), mimetype="application/json")


@restapi.route("/data/<string:uuid>", methods=["GET"])
def get_data(uuid):
    log.info("Request details: %s", json.dumps(request.args.to_dict(flat=False)))
    start_date = _verify_datatime_param(
        "start_date", request.args.get("start_date", default=MIN_DATE)
    )
    end_date = _verify_datatime_param(
        "end_date",
        request.args.get(
            "end_date",
            default=datetime.datetime.now(datetime.timezone.utc).strftime(DATE_FORMAT),
        ),
    )

    columns = request.args.getlist("columns") or None

    result: Optional[pd.DataFrame] = app.api.get_dataset_data(
        uuid=uuid, date_start=start_date, date_end=end_date, columns=columns
    )

    # if result is None, return empty response
    if result is None:
        return Response("[]", mimetype="application/json")

    start_depth = _verify_depth_param(
        "start_depth", numpy.double(request.args.get("start_depth", default=-1.0))
    )
    end_depth = _verify_depth_param(
        "end_depth", numpy.double(request.args.get("end_depth", default=-1.0))
    )

    is_to_index = _verify_to_index_flag_param(
        request.args.get("is_to_index", default=None)
    )

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

    log.info("Record number return %s for query", len(filtered.index))

    log.info("Memory usage: %s", get_memory_usage_percentage())

    f = request.args.get("format", default="json", type=str)
    if f == "json":
        # Depends on whether receiver support gzip encoding
        compress = "gzip" in request.headers.get("Accept-Encoding", "")
        if is_to_index:
            return _response_partial_json(filtered, compress)
        else:
            return _response_json(filtered, compress)
    elif f == "netcdf":
        return _response_netcdf(filtered)


def get_memory_usage_percentage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    total_memory = psutil.virtual_memory().total
    log.info("Total memory: %s", total_memory)
    return (memory_info.rss / total_memory) * 100
