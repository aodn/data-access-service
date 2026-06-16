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


def generate_pmtiles_for_all_parquets(api: BaseAPI):
    aws = AWSHelper()
    metadata_list = api.get_mapped_meta_data(uuid=None)
    ensure_tippecanoe()
    uuid_dname_pair = []

    for k, v in metadata_list.items():
        dataset_names = v.keys()
        for dataset_name in dataset_names:
            # only process parquets
            if dataset_name.endswith(".parquet"):
                uuid_dname_pair.append((k, dataset_name))

    logger.info(f"{len(uuid_dname_pair)} datasets to process.")
    index = 1
    for uuid, dname in uuid_dname_pair:
        try:
            logger.info(
                f"Start generating PMTiles for uuid: {uuid}, dataset: {dname}. Process: {index}/{len(uuid_dname_pair)}"
            )

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
                        "havier-example-bucket",
                        f"visualization/{uuid}/{dname}.pmtiles",
                    )
                    logger.info(
                        f"Pmtiles file of dataset {dname}, uuid {uuid} uploaded to S3."
                    )
            index += 1
        except Exception as e:
            logger.error(f"Error processing dataset {uuid}, parquet {dname}: {e}")
            index += 1


def get_visualization_style(uuid: str, dname: str) -> PmtilesVisualizationStyle:
    # currently Hexagon is the default style. May need more styles in the future according to the uuid and dname
    return PmtilesVisualizationStyle.HEXAGONS


# Because tippecanoe is only used in batch, installing it in the Docker image is not ideal as it will increase the image size.
def ensure_tippecanoe():
    import shutil
    import subprocess

    if shutil.which("tippecanoe"):
        return

    subprocess.run(["apt-get", "update"], check=True)

    subprocess.run(
        [
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "tippecanoe",
        ],
        check=True,
    )
