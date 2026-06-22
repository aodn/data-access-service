import tempfile

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.api import BaseAPI
from .processors.hexbin_processor import HexbinProcessor
from ...models.pmtiles_types import (
    PmtilesVisualizationStyle,
)

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()


def generate_pmtiles_for_all_parquets(api: BaseAPI):
    metadata_list = api.get_mapped_meta_data(uuid=None)

    for k, v in metadata_list.items():
        dataset_names = v.keys()
        for dataset_name in dataset_names:
            # only process parquets
            if dataset_name.endswith(".parquet"):
                generate_pmtiles_for_parquets(api, k, dataset_name)


def generate_pmtiles_for_parquets(api: BaseAPI, uuid: str, dname: str):

    try:
        logger.info(f"Start generating PMTiles for uuid: {uuid}, dataset: {dname}")

        # Do everything in a temp directory to avoid filling up disk space.
        # The temp directory and all its contents will be automatically deleted after the with block.
        with tempfile.TemporaryDirectory() as tempdirname:

            vis_style = get_visualization_style(uuid=uuid, dname=dname)

            if vis_style == PmtilesVisualizationStyle.HEXAGONS:
                logger.info("Visualization style: HEXAGONS")
                hex_processor = HexbinProcessor(
                    work_dir=tempdirname, uuid=uuid, dataset_name=dname, api=api
                )
                logger.info("Hexbin Processor has been initialized.")
                pmtiles_path = hex_processor.process()
                # TODO: please use functions like is_local_pmtiles_valid() in pmtiles_util to verify the new generated pmtiles file
                #  is valid or not before uploading to S3. We don't want to upload an invalid pmtiles file to S3 and cause errors
                aws.upload_file_to_s3(
                    pmtiles_path,
                    config.get_pmtiles_config().bucket_name,
                    f"visualization/{uuid}/{dname}.pmtiles",
                )
                logger.info(
                    f"Pmtiles file of dataset {dname}, uuid {uuid} uploaded to S3."
                )
    except Exception as e:
        logger.error(f"Error processing dataset {uuid}, parquet {dname}: {e}")


def get_visualization_style(uuid: str, dname: str) -> PmtilesVisualizationStyle:
    # currently Hexagon is the default style. May need more styles in the future according to the uuid and dname
    return PmtilesVisualizationStyle.HEXAGONS
