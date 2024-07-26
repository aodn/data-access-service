import dataclasses
from flask import Blueprint, request
from . import app

restapi = Blueprint('restapi', __name__)


@restapi.route('/metadata/<string:uuid>', methods=['GET'])
def get_mapped_metadata(uuid):
    return dataclasses.asdict(app.api.get_mapped_meta_data(uuid))


@restapi.route('/metadata/<string:uuid>/raw', methods=['GET'])
def get_raw_metadata(uuid):
    return app.api.get_raw_meta_data(uuid)


@restapi.route('/data/<string:uuid>', methods=['GET'])
def get_data(uuid):
    return app.api.get_dataset_data(
        uuid=uuid,
        date_start=request.args.get('start', default=None, type=str),
        date_end=request.args.get('end', default=None, type=str)
    )
