import shutil

import dask.dataframe
import pandas as pd
import pytest

from pathlib import Path
from unittest.mock import patch
from data_access_service.core.AWSHelper import AWSHelper
from aodn_cloud_optimised.lib import DataQuery
from data_access_service import Config
from data_access_service.tasks.data_collection import collect_data_files
from data_access_service.tasks.generate_dataset import (
    process_data_files,
)
from tests.batch.batch_test_consts import INIT_JOB_ID
from tests.core.test_with_s3 import TestWithS3, REGION


class TestKeyMapping(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        """
        This will call only once, so you should not delete any update in any test case
        :param mock_boto3_client:
        :param setup_resources:
        :param aws_clients:
        :return:
        """
        s3_client, _, _ = aws_clients
        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample_edge_cases",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_special_time_column(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:
                    # Job 1, use different job id to avoid read same folder
                    process_data_files(
                        job_id_of_init="888",
                        job_index="1",
                        intermediate_output_folder=config.get_temp_folder("888"),
                        uuid="4402cb50-e20a-44ee-93e6-4728259250d2",
                        keys=["argo.parquet"],
                        start_date=pd.Timestamp("2000-01-01 00:00:00"),
                        end_date=pd.Timestamp("2013-01-01 23:59:59.999999999"),
                        multi_polygon=None,
                    )
                    names = helper.list_s3_folders(
                        config.get_csv_bucket_name(),
                        f"{config.get_s3_temp_folder_name('888')}argo.parquet",
                    )
                    assert "part-1" in names, "part-1 not exit!"

                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name('888')}argo.parquet"
                    subset = helper.read_parquet_from_s3(target_path)
                    assert len(subset) == 44364
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_special_time_dimension(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        this dataset has a special TIME dimension
        """
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:

                    dname = "satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.zarr"

                    process_data_files(
                        job_id_of_init="888",
                        job_index="1",
                        intermediate_output_folder=config.get_temp_folder("888"),
                        uuid="2d496463-600c-465a-84a1-8a4ab76bd505",
                        keys=[dname],
                        start_date=pd.Timestamp("2015-01-31 00:00:00.000000000"),
                        end_date=pd.Timestamp("2015-02-01 23:59:59.999999999"),
                        multi_polygon=None,
                    )
                    names = helper.list_s3_folders(
                        config.get_csv_bucket_name(),
                        f"{config.get_s3_temp_folder_name('888')}{dname}",
                    )
                    assert "part-1.zarr" in names, "part-1 not exit!"
                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name('888')}{dname}/part-*.zarr"
                    subset = helper.read_multipart_zarr_from_s3(target_path)

                    assert len(subset["time"]) == 2
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)
