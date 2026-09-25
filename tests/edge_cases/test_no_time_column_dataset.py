"""Download of a real parquet dataset that has no time column.

diver_site_information_qc is partitioned only by `polygon`. The canned copy in
s3_sample_edge_cases holds the real `_common_metadata` and 6 of its polygon
partitions, taken from s3://aodn-cloud-optimised/diver_site_information_qc.parquet/.
"""

import json
import math
import shutil
from pathlib import Path
from typing import Final
from unittest.mock import patch

import pyarrow.dataset as pa_ds
import pytest
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import API, Config
from data_access_service.batch.subsetting import init, prepare_data
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.models.subset_request import NON_SPECIFIED
from tests.core.result_check import assert_same_rows
from tests.core.test_with_s3 import TestWithS3, REGION

CANNED: Final = Path(__file__).parent.parent / "canned/s3_sample_edge_cases"
DIVER_KEY: Final = "diver_site_information_qc.parquet"
DIVER_UUID: Final = "e41efa35-03f0-4dea-be98-2a69e46b510b"
INIT_JOB_ID: Final = "9174-no-time-column"
POLYGON_COUNT_PER_JOB: Final = 2


def _canned_rows():
    dataset = pa_ds.dataset(
        str(CANNED / DIVER_KEY),
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )
    return dataset.to_table().to_pandas()


class TestNoTimeColumnDataset(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT, CANNED)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_download_is_split_by_polygon_partition(
        self,
        aws_clients,
        setup_resources,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        source = _canned_rows()
        polygon_count = source["polygon"].nunique()

        api = API()
        api.initialize_metadata()
        assert api.resolve_dim_names(DIVER_UUID, DIVER_KEY)[2] is None

        init_parameters = {
            Parameters.UUID.value: DIVER_UUID,
            Parameters.KEY.value: DIVER_KEY,
            Parameters.START_DATE.value: NON_SPECIFIED,
            Parameters.END_DATE.value: NON_SPECIFIED,
            Parameters.MULTI_POLYGON.value: NON_SPECIFIED,
            Parameters.RECIPIENT.value: "test@example.com",
            Parameters.OUTPUT_FORMAT.value: "csv",
        }

        try:
            with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
                with (
                    patch.object(
                        Config,
                        "get_polygon_count_per_job",
                        return_value=POLYGON_COUNT_PER_JOB,
                    ),
                    patch.object(
                        AWSHelper, "submit_a_job", return_value="preparation-job"
                    ) as submit_a_job,
                    patch.object(AWSHelper, "send_email") as send_email,
                ):
                    # Job 1: init splits the partitions instead of the dates
                    init(api, INIT_JOB_ID, init_parameters)

                    send_email.assert_not_called()
                    preparation = submit_a_job.call_args_list[0].kwargs
                    preparation_parameters = preparation["parameters"]
                    assert Parameters.DATE_RANGES.value not in preparation_parameters
                    polygon_ranges = json.loads(
                        preparation_parameters[Parameters.POLYGON_RANGES.value]
                    )
                    expected_children = math.ceil(polygon_count / POLYGON_COUNT_PER_JOB)
                    assert len(polygon_ranges) == expected_children
                    assert preparation["array_size"] == expected_children

                    # Job 2: every child of the array job
                    for job_index in polygon_ranges:
                        prepare_data(
                            api,
                            job_index=job_index,
                            parameters=preparation_parameters,
                        )

                helper = AWSHelper()
                names = helper.list_s3_folders(
                    config.get_subsetting_bucket_name(),
                    f"{config.get_s3_temp_folder_name(INIT_JOB_ID)}{DIVER_KEY}",
                )
                for job_index in polygon_ranges:
                    assert f"part-{job_index}" in names, names

                subset = helper.read_parquet_from_s3(
                    f"s3://{config.get_subsetting_bucket_name()}/"
                    f"{config.get_s3_temp_folder_name(INIT_JOB_ID)}{DIVER_KEY}"
                ).compute()

                # Every row once: no child re-reads another child's partitions
                assert subset["site_code"].is_unique
                assert_same_rows(
                    subset,
                    source,
                    ["site_code", "site_name", "latitude", "longitude", "location"],
                )
        finally:
            shutil.rmtree(config.get_temp_folder(INIT_JOB_ID), ignore_errors=True)
