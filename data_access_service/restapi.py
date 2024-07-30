import dataclasses
import datetime
import json
import logging
import os
import tempfile

import dask.dataframe
import xarray as xr
import numpy
import pandas as pd

from typing import Optional
from dask.dataframe import dd
from flask import Blueprint, request, abort, Response, send_file
from dateutil import parser
from http import HTTPStatus

from pandas import DataFrame

from . import app
from .api import gzip_compress
from .error import ErrorResponse

restapi = Blueprint('restapi', __name__)
log = logging.getLogger(__name__)

RECORD_PER_PARTITION: Optional[int] = 1000


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json(dask, compress: bool = False):
    for partition in dask.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient='records'):
            yield gzip_compress(json.dumps(record) + '\n') if compress else json.dumps(record) + '\n'


def _verify_datatime_param(name: str, req_date: str) -> datetime:
    _date = None

    try:
        if req_date is not None:
            _date = parser.isoparse(req_date)

    except (ValueError, TypeError) as e:
        error_message = ErrorResponse(status_code=HTTPStatus.BAD_REQUEST,
                                      details=f'Incorrect format [{name}]',
                                      parameters=f'{name}={req_date}')

        abort(HTTPStatus.BAD_REQUEST, error_message)

    return _date


def _verify_depth_param(name: str, req_value: numpy.double) -> numpy.double | None:
    if req_value is not None and req_value > 0.0:
        error_message = ErrorResponse(status_code=HTTPStatus.BAD_REQUEST,
                                      details=f'Depth cannot greater than zero',
                                      parameters=f'{name}={req_value}')

        abort(HTTPStatus.BAD_REQUEST, error_message)
    else:
        return req_value


def _response_json(filtered: DataFrame, compress: bool):
    ddf: dask.dataframe.DataFrame = dd.from_pandas(filtered, npartitions=len(filtered.index) // RECORD_PER_PARTITION + 1)
    if compress:
        response = Response(_generate_json(ddf, True), mimetype='application/json')
        response.headers['Content-Encoding'] = 'gzip'
    else:
        response = Response(_generate_json(ddf), mimetype='application/json')

    return response


# TODO: Need to use the metadata to assign correct type to netcdf, right now field type is wrong
def _response_netcdf(filtered: DataFrame):

    # Convert the DataFrame to an xarray Dataset
    ds = xr.Dataset.from_dataframe(filtered)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as tmp_file:
        tmp_file_name = tmp_file.name
        ds.to_netcdf(tmp_file_name)

    # Send the file as a response to the client
    response = send_file(tmp_file_name, as_attachment=True, download_name='output.nc', mimetype='application/x-netcdf')

    # Clean up the temporary file after sending the response
    def _remove_file_if_exists(file_path):
        """ Helper function to remove file if it exists """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error removing file {file_path}: {e}")

    response.call_on_close(lambda: _remove_file_if_exists(tmp_file_name))

    return response


@restapi.route('/metadata/<string:uuid>', methods=['GET'])
def get_mapped_metadata(uuid):
    return dataclasses.asdict(app.api.get_mapped_meta_data(uuid))


@restapi.route('/metadata/<string:uuid>/raw', methods=['GET'])
def get_raw_metadata(uuid):
    return app.api.get_raw_meta_data(uuid)


@restapi.route('/data/<string:uuid>', methods=['GET'])
def get_data(uuid):
    start_date = _verify_datatime_param('start_date', request.args.get('start_date', default=None, type=str))
    end_date = _verify_datatime_param('end_date', request.args.get('end_date', default=None, type=str))

    result: Optional[pd.DataFrame] = app.api.get_dataset_data(
        uuid=uuid,
        date_start=start_date,
        date_end=end_date
    )

    start_depth = _verify_depth_param('start_depth', request.args.get('start_depth', default=None, type=numpy.double))
    end_depth = _verify_depth_param('end_depth', request.args.get('end_depth', default=None, type=numpy.double))

    # The cloud optimized format is fast to lookup if there is an index, some field isn't part of the
    # index and therefore will not gain to filter by those field, indexed fields are site_code, timestamp, polygon

    # Depth is below sea level zero, so logic slightly diff
    if start_depth is not None and end_depth is not None:
        filtered = result[(result['DEPTH'] <= start_depth) & (result['DEPTH'] >= end_depth)]
    elif start_depth is not None:
        filtered = result[(result['DEPTH'] <= start_depth)]
    elif end_depth is not None:
        filtered = result[result['DEPTH'] >= end_depth]
    else:
        filtered = result

    log.info('Record number return %s for query', len(filtered.index))

    f = request.args.get('format', default='json', type=str)
    if f == 'json':
        # Depends on whether receiver support gzip encoding
        compress = 'gzip' in request.headers.get('Accept-Encoding', '')
        return _response_json(filtered, compress)

    elif f == 'netcdf':
        return _response_netcdf(filtered)

