import dataclasses
import json
import logging
from typing import Optional

import numpy
import pandas as pd
from dask.dataframe import dd
from flask import Blueprint, request, abort, Response
from dateutil import parser
from http import HTTPStatus
from . import app
from .error import ErrorResponse

restapi = Blueprint('restapi', __name__)
log = logging.getLogger(__name__)


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        else:
            return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json(dask):
    for partition in dask.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient='records'):
            yield json.dumps(record) + '\n'


def _verify_datatime_param(name: str, req_date: str):
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


def _verify_depth_param(name: str, req_value: numpy.double):
    if req_value <= 0:
        return req_value
    else:
        error_message = ErrorResponse(status_code=HTTPStatus.BAD_REQUEST,
                                      details=f'Depth cannot greater than zero',
                                      parameters=f'{name}={req_value}')

        abort(HTTPStatus.BAD_REQUEST, error_message)

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

    # The cloud optimized format is fast to lookup if there is an index, some field isn't part of the
    # index and therefore will not gain to filter by those field, indexed fields are site_code, timestamp, polygon
    ddf = dd.from_pandas(filtered, npartitions=10)

    return Response(_generate_json(ddf), mimetype='application/json')
