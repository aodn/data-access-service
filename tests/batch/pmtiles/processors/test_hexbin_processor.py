import json
import os
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import gzip
import pandas
import pytest
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
from data_access_service.batch.pmtiles.processors.hexbin_processor import (
    HexbinProcessor,
)
from data_access_service.models.pmtiles_types import TimeGroupBy
from data_access_service.utils.pmtiles_utils import open_pmtiles
from tests.core.test_with_s3 import TestWithS3, REGION

ANIMAL_ACOUSTIC_UUID = "541d4f15-122a-443d-ab4e-2b5feb08d6a0"
ANIMAL_ACOUSTIC_DNAME = "animal_acoustic_tracking_delayed_qc.parquet"

# Expected (h_high, YYYYMMDD) -> count for animal_acoustic in s3_sample2
# (max H3 res 8). Source: 22 detections — 20 on 2012-11-07, 2 on 2014-07-01.
EXPECTED_STAGED_DATE_AGGREGATES = {
    ("88be8d9819fffff", 20121107): 20,
    ("88be0e373dfffff", 20140701): 2,
}


class TestHexbinProcessor(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent.parent.parent / "canned/s3_sample2",
        )

    def _configure_hex_processor(
        self, tempdirname, api, localstack, time_group_by: TimeGroupBy | None = None
    ):
        hex_processor = HexbinProcessor(
            uuid=ANIMAL_ACOUSTIC_UUID,
            dataset_name=ANIMAL_ACOUSTIC_DNAME,
            work_dir=tempdirname,
            api=api,
        )
        if time_group_by is not None:
            hex_processor.pmtiles_config = replace(
                hex_processor.pmtiles_config, time_group_by=time_group_by
            )
        hex_processor.pm_client.execute(
            f"""
                    SET s3_endpoint='{localstack.get_url().replace("http://", "")}';
                    SET s3_region='{REGION}';
                    SET s3_access_key_id='test';
                    SET s3_secret_access_key='test';
                    SET s3_url_style='path';
                    SET s3_use_ssl=false;
                """
        )
        return hex_processor

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_hexbin_processor(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
        localstack,
    ):
        config = Config.get_config()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):

            with tempfile.TemporaryDirectory() as tempdirname:

                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname, api, localstack
                    )
                    assert (
                        hex_processor.pmtiles_config.time_group_by == TimeGroupBy.MONTH
                    )

                    # Run the pipeline steps individually, in the same order as
                    # process(), so intermediate outputs can be asserted before
                    # the cleanup steps remove them.
                    hex_processor.build_staging_parquet()

                    staged_parquet_path = hex_processor.get_staged_path()

                    df = pandas.read_parquet(staged_parquet_path)
                    assert not df.empty, "df is empty"
                    assert list(df.columns) == [
                        "h_high",
                        "ym",
                        "c",
                    ], "df columns are not correct"
                    assert df[["h_high", "ym"]].duplicated().sum() == 0

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()

                    geojsonseq_dir = hex_processor.get_geojsonseq_dir()

                    total_features = 0

                    for root, _, files in os.walk(geojsonseq_dir):
                        for file in files:
                            if file.endswith(".geojsonseq.gz"):
                                path = os.path.join(root, file)

                                with gzip.open(path, "r") as f:
                                    total_features += sum(1 for _ in f)

                    assert (
                        total_features == 12
                    ), f"total features should be 12 based on the test data, but got {total_features}"

                    hex_processor._remove_staged_parquet()
                    assert not os.path.exists(
                        staged_parquet_path
                    ), "staged parquet should be removed before tippecanoe"

                    hex_processor.generate_pmtiles_file(
                        geojsonseq_paths=geojsonseq_paths
                    )

                    # check pmtiles file exists
                    pmtiles_path = hex_processor.get_output_pmtiles_path()
                    assert os.path.exists(
                        pmtiles_path
                    ), f"pmtiles file not exists at {pmtiles_path}"

                    hex_processor._remove_geojsonseq_files(geojsonseq_paths)
                    for path in geojsonseq_paths:
                        assert not os.path.exists(
                            path
                        ), "geojsonseq files should be removed after pmtiles generation"

                    with open_pmtiles(pmtiles_path) as reader:
                        header = reader.header()
                        metadata = reader.metadata()

                        hex_processor.logger.info("metadata:")
                        hex_processor.logger.info(metadata)

                        assert header["min_zoom"] == 0, "min zoom should be 0"
                        assert header["max_zoom"] == 12, "max zoom should be 12"
                        assert (
                            "vector_layers" in metadata
                        ), "metadata should contain vector_layers"
                        assert (
                            len(metadata["vector_layers"]) > 0
                        ), "vector_layers should not be empty"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_animal_acoustic_aggregates_by_date(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        localstack,
    ):
        """
        Verify day-level (YYYYMMDD) count aggregation when time_group_by=date
        for animal_acoustic_tracking_delayed_qc.parquet from s3_sample2.
        """
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname,
                        api,
                        localstack,
                        time_group_by=TimeGroupBy.DATE,
                    )
                    hex_processor.build_staging_parquet()

                    df = pandas.read_parquet(hex_processor.get_staged_path())
                    assert list(df.columns) == ["h_high", "d", "c"]
                    assert ((df["d"] >= 10000101) & (df["d"] <= 99991231)).all()

                    actual = {
                        (str(row.h_high), int(row.d)): int(row.c)
                        for row in df.itertuples(index=False)
                    }
                    assert actual == EXPECTED_STAGED_DATE_AGGREGATES, (
                        f"staged (h_high, d) counts mismatch.\n"
                        f"expected={EXPECTED_STAGED_DATE_AGGREGATES}\n"
                        f"actual={actual}"
                    )
                    assert sum(actual.values()) == 22

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()
                    high_res_path = next(
                        p
                        for p in geojsonseq_paths
                        if p.endswith("hex_z10.geojsonseq.gz")
                    )
                    date_counts_by_cell = {}
                    with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            feature = json.loads(line)
                            props = feature["properties"]
                            cell = props["h"]
                            date_counts_by_cell[cell] = {
                                int(k[1:]): int(v)
                                for k, v in props.items()
                                if k.startswith("m") and k[1:].isdigit()
                            }

                    expected_by_cell = {}
                    for (h_high, d), c in EXPECTED_STAGED_DATE_AGGREGATES.items():
                        expected_by_cell.setdefault(h_high, {})[d] = c

                    assert date_counts_by_cell == expected_by_cell, (
                        f"geojsonseq date properties mismatch.\n"
                        f"expected={expected_by_cell}\n"
                        f"actual={date_counts_by_cell}"
                    )
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)


def test_time_group_by_default_and_invalid():
    config = Config.get_config()
    pm = config.get_pmtiles_config()
    assert pm.time_group_by == TimeGroupBy.MONTH

    original = config.config["pmtiles"]["config"].get("time_group_by")
    try:
        config.config["pmtiles"]["config"]["time_group_by"] = "week"
        with pytest.raises(ValueError, match="time_group_by"):
            config.get_pmtiles_config()
    finally:
        if original is None:
            config.config["pmtiles"]["config"].pop("time_group_by", None)
        else:
            config.config["pmtiles"]["config"]["time_group_by"] = original


def test_build_time_key_expressions():
    from data_access_service.core.duckdbclient import PmTileDuckDBClient

    ym = PmTileDuckDBClient.build_ym_expression("TIMESTAMP", "timestamp")
    assert "%Y%m" in ym
    assert "%Y%m%d" not in ym

    d = PmTileDuckDBClient.build_date_key_expression("TIMESTAMP", "timestamp")
    assert "%Y%m%d" in d
