import os
import json
import datetime
import tempfile

import xarray as xr
import dask.dataframe as dd

from dateutil import parser
from http import HTTPStatus
from pandas import DataFrame

from fastapi.responses import FileResponse, Response
from starlette.background import BackgroundTask
from fastapi import HTTPException, status

from data_access_service.common.constants import RECORD_PER_PARTITION
from data_access_service.core.api import gzip_compress
from data_access_service.core.error import ErrorResponse


# Make all non-numeric and str field to str so that json do not throw serializable error
def _convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json(dask, compress: bool = False):
    for partition in dask.to_delayed():
        partition_df = _convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient="records"):
            yield (
                gzip_compress(json.dumps(record) + "\n")
                if compress
                else json.dumps(record) + "\n"
            )


def verify_datatime_param(name: str, req_date: str) -> datetime:
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )

    return _date


def verify_depth_param(name: str, req_value: float) -> float | None:
    if req_value is not None and req_value > 0.0:
        error_message = ErrorResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            details=f"Depth cannot greater than zero",
            parameters=f"{name}={req_value}",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )
    else:
        return req_value


def response_json(filtered: DataFrame, compress: bool):
    ddf: dd.DataFrame = dd.from_pandas(
        filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1
    )
    if compress:
        content = _generate_json(ddf, True)
        return Response(
            content, media_type="application/json", headers={"Content-Encoding": "gzip"}
        )
    else:
        content = _generate_json(ddf)
        return Response(content, media_type="application/json")


# TODO: Need to use the metadata to assign correct type to netcdf, right now field type is wrong
def response_netcdf(filtered: DataFrame):
    # Convert the DataFrame to a xarray Dataset
    ds = xr.Dataset.from_dataframe(filtered)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp_file:
        tmp_file_name = tmp_file.name
        ds.to_netcdf(tmp_file_name)

    # Create the FileResponse
    response = FileResponse(
        path=tmp_file_name,
        filename="output.nc",
        media_type="application/x-netcdf",
        background=BackgroundTask(os.remove, tmp_file_name),
    )

    return response
