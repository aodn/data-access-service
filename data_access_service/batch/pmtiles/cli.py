from typing import List
import time
from data_access_service.core.api import BaseAPI
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service import Config, init_log
from .types import HexLayerSpec
from .tippecanoe import generate_pmtiles
from data_access_service.core.constants import (
    STR_LONGITUDE_UPPER_CASE,
    STR_LATITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)
from aodn_cloud_optimised.lib.DataQuery import BUCKET_OPTIMISED_DEFAULT


def generate_pmtiles_for_all_parquets(api: BaseAPI):
    aws = AWSHelper()
    metadata_list = api.get_mapped_meta_data(uuid=None)

    uuid_dname_pair = []
    for k, v in metadata_list.items():
        dataset_names = v.keys()
        for dataset_name in dataset_names:
            uuid_dname_pair.append((k, dataset_name))

    for uuid, dname in uuid_dname_pair:
        try:
            test_file_path = "./test_file.txt"
            with open(test_file_path, "w") as test_file:
                test_file.write(
                    f"This is a test file for pmtiles generation. Dataset UUID: {uuid}, Parquet Name: {dname}\n"
                )

            aws.upload_file_to_s3(
                test_file_path,
                "havier-example-bucket",
                f"visualization/{uuid}/{dname}.txt",
            )
            aws.upload_file_to_s3(
                test_file_path,
                "aodn-cloud-optimized-subset-edge",
                f"visualization/{uuid}/{dname}.txt",
            )
        except Exception as e:
            logger = init_log(Config.get_config())
            logger.error(f"Error processing dataset {uuid!r}, parquet {dname!r}: {e}")


def generate_pmtiles_for(dataset_uuid: str, parquet_name: str, api: BaseAPI):
    logger = init_log(Config.get_config())
    logger.info(
        f"generate_pmtiles_for: uuid={dataset_uuid!r}, parquet={parquet_name!r}"
    )

    if not isinstance(parquet_name, str) or not parquet_name.endswith(".parquet"):
        raise ValueError("parquet_name must be a string ending with '.parquet'")

    s3_uri = f"s3://{BUCKET_OPTIMISED_DEFAULT}/{parquet_name}/**/*.parquet"
    parquet_stem = parquet_name.removesuffix(".parquet")

    layers: List[HexLayerSpec] = [
        HexLayerSpec(
            name="hex_z0",
            h3_resolution=2,
            minzoom=0,
            maxzoom=1,
            output_path=f"data/intermediate/{parquet_stem}_hex_z0.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_z2",
            h3_resolution=3,
            minzoom=2,
            maxzoom=3,
            output_path=f"data/intermediate/{parquet_stem}_hex_z2.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_z4",
            h3_resolution=4,
            minzoom=4,
            maxzoom=5,
            output_path=f"data/intermediate/{parquet_stem}_hex_z4.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_z6",
            h3_resolution=6,
            minzoom=6,
            maxzoom=7,
            output_path=f"data/intermediate/{parquet_stem}_hex_z6.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_z8",
            h3_resolution=7,
            minzoom=8,
            maxzoom=9,
            output_path=f"data/intermediate/{parquet_stem}_hex_z8.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_z10",
            h3_resolution=8,
            minzoom=10,
            maxzoom=12,
            output_path=f"data/intermediate/{parquet_stem}_hex_z10.geojsonseq",
        ),
    ]

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
    generate_pmtiles(
        parquet_s3_uri=s3_uri,
        output_pmtiles=f"data/output/{parquet_stem}.pmtiles",
        layers=layers,
        lon_col=lon_col,
        lat_col=lat_col,
        time_col=time_col,
        staged_parquet_dir=f"data/parquet_staged/{parquet_stem}",
        memory_limit="20GB" if Config.get_config().is_batch() else "4GB",
        threads=8 if Config.get_config().is_batch() else 4,
        fetch_size=500_000 if Config.get_config().is_batch() else 100_000,
    )
    logger.info(f"generate_pmtiles_for: finished in {time.time() - t0:.1f}s")
