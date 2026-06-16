import tempfile
from typing import List
import time
from data_access_service.core.api import BaseAPI
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service import Config, init_log
from .processors.hexbin_processor import HexbinProcessor
from .tippecanoe import generate_pmtiles
from data_access_service.core.constants import (
    STR_LONGITUDE_UPPER_CASE,
    STR_LATITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)
from aodn_cloud_optimised.lib.DataQuery import BUCKET_OPTIMISED_DEFAULT

from ...models.pmtiles_types import (
    HexLayerSpec,
    PmtilesVisualizationStyle,
    PmtilesLayerSpec,
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

    for uuid, dname in uuid_dname_pair:
        try:
            logger.info(
                "Start PMTiles processing of uuid: "
                + uuid
                + ",  dataset name "
                + dname
                + ""
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
                    aws.upload_file_to_s3(
                        pmtiles_path,
                        "havier-example-bucket",
                        f"visualization/{uuid}/{dname}.pmtiles",
                    )

        except Exception as e:
            logger.error(f"Error processing dataset {uuid}, parquet {dname}: {e}")


def get_visualization_style(uuid: str, dname: str) -> PmtilesVisualizationStyle:
    # currently Hexagon is the default style. May need more styles in the future according to the uuid and dname
    return PmtilesVisualizationStyle.HEXAGONS


def generate_pmtiles_for(
    dataset_uuid: str,
    parquet_name: str,
    layers: List[PmtilesLayerSpec],
    work_dir: str,
    api: BaseAPI,
) -> str:
    logger.debug(
        f"generate_pmtiles_for: uuid={dataset_uuid!r}, parquet={parquet_name!r}"
    )

    if not isinstance(parquet_name, str) or not parquet_name.endswith(".parquet"):
        raise ValueError("parquet_name must be a string ending with '.parquet'")

    s3_uri = f"s3://{BUCKET_OPTIMISED_DEFAULT}/{parquet_name}/**/*.parquet"

    lon_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_LONGITUDE_UPPER_CASE]
    )
    lat_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_LATITUDE_UPPER_CASE]
    )
    time_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_TIME_UPPER_CASE]
    )

    if not lon_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no LONGITUDE column found in dataset metadata."
        )
    if not lat_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no LATITUDE column found in dataset metadata."
        )
    if not time_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no TIME column found in dataset metadata. "
            f"This dataset may not be a time-series dataset (e.g. it is a site information table). "
            f"pmtiles generation requires a time column to aggregate monthly observation counts."
        )

    lon_col = lon_col_mapped[0]
    lat_col = lat_col_mapped[0]
    time_col = time_col_mapped[0]
    logger.info(
        f"generate_pmtiles_for: mapped columns — lon={lon_col!r}, lat={lat_col!r}, time={time_col!r}"
    )

    t0 = time.time()
    pmtiles_generation_config = Config.get_config().get_pmtiles_generation_config(
        parquet_name
    )
    abs_pmtiles_file_path = generate_pmtiles(
        parquet_s3_uri=s3_uri,
        output_pmtiles=f"{work_dir}/{pmtiles_generation_config.output_pmtiles_dir}",
        layers=layers,
        lon_col=lon_col,
        lat_col=lat_col,
        time_col=time_col,
        staged_parquet_dir=f"{work_dir}/{pmtiles_generation_config.staged_parquet_dir}",
        geojsonseq_dir=f"{work_dir}/{pmtiles_generation_config.geojsonseq_dir}",
        duckdb_temp_dir=f"{work_dir}/{pmtiles_generation_config.duckdb_temp_dir}",
        memory_limit=pmtiles_generation_config.memory_limit,
        threads=pmtiles_generation_config.threads,
        fetch_size=pmtiles_generation_config.fetch_size,
    )
    logger.info(f"generate_pmtiles_for: finished in {time.time() - t0:.1f}s")

    return abs_pmtiles_file_path


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
