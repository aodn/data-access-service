import dataclasses
import json

from dask.dataframe import dd
from flask import Blueprint, request, abort, Response
from dateutil import parser
from http import HTTPStatus
from . import app
from .error import ErrorResponse

restapi = Blueprint('restapi', __name__)


# Make all non-numeric and str field to str so that json do not throw serializable error
def convert_non_numeric_to_str(df):
    def convert_value(value):
        if not isinstance(value, (int, float, str)):
            return str(value)
        return value

    return df.map(convert_value)


# Function to generate JSON lines from Dask DataFrame
def _generate_json(dask):
    for partition in dask.to_delayed():
        partition_df = convert_non_numeric_to_str(partition.compute())
        for record in partition_df.to_dict(orient='records'):
            yield json.dumps(record) + '\n'


@restapi.route('/metadata/<string:uuid>', methods=['GET'])
def get_mapped_metadata(uuid):
    return dataclasses.asdict(app.api.get_mapped_meta_data(uuid))


@restapi.route('/metadata/<string:uuid>/raw', methods=['GET'])
def get_raw_metadata(uuid):
    return app.api.get_raw_meta_data(uuid)


@restapi.route('/data/<string:uuid>', methods=['GET'])
def get_data(uuid):
    req_start_date = request.args.get('start', default=None, type=str)
    start_date = None

    try:
        if req_start_date is not None:
            start_date = parser.isoparse(req_start_date)

    except (ValueError, TypeError) as e:
        error_message = ErrorResponse(status_code=HTTPStatus.BAD_REQUEST,
                                      details='Incorrect format [start]',
                                      parameters='start=' + req_start_date)

        abort(HTTPStatus.BAD_REQUEST, error_message)

    result = app.api.get_dataset_data(
        uuid=uuid,
        date_start=start_date,
        date_end=request.args.get('end', default=None, type=str)
    )

    ddf = dd.from_pandas(result, npartitions=10)

    return Response(_generate_json(ddf), mimetype='application/json')
