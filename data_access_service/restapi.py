import logging
from flask import Blueprint
from aodn_cloud_optimised import ParquetDataQuery

restapi = Blueprint('restapi', __name__)
logger = logging.getLogger(__name__)


class API:
    def __init__(self):
        logger.info("Init parquet data query instance")

        self.instance = ParquetDataQuery.GetAodn()
        self.metadata = self.instance.get_metadata()

        # We need to create the mapping
        self.create_uuid_dataset_map()

        logger.info("Done init")

    # Do not use cache, so that we can refresh it again
    def create_uuid_dataset_map(self):
        catalog = self.metadata.metadata_catalog_uncached()

    def get_meta_data(self):
        return "hi"


api = API()


@restapi.route('/metadata/<string:uuid>', methods=['GET'])
def get_metadata(uuid):
    return api.get_meta_data()
