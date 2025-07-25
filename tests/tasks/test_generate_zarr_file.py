import shutil
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


class TestGenerateZarrFile(TestWithS3):

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
            Path(__file__).parent.parent / "canned/s3_sample2",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_partition_processing_with_single_zarr(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        Test the process_data_files function by doing a single job index 10, this make sure
        we can process zarr file by writing to and single partition, that is part-10.zarr in this case
        """
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        # This uuid contains two dataset in the canned data
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_delayed_qc.zarr
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_derived_product.zarr
        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:
                    process_data_files(
                        job_id_of_init=INIT_JOB_ID,
                        job_index="10",
                        intermediate_output_folder=config.get_temp_folder(INIT_JOB_ID),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-07-01 00:00:00"),
                        end_date=pd.Timestamp("2011-09-01 00:00:00"),
                        multi_polygon=None,
                    )
                    # This is a zarr file, we should be able to read the result from S3
                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name(INIT_JOB_ID)}vessel_satellite_radiance_delayed_qc.zarr/part-*.zarr"
                    data = helper.read_multipart_zarr_from_s3(target_path)
                    assert len(data["TIME"]) == 158902, "file have enough data"

                    # At least we can convert it to netcdf
                    collect_data_files(
                        INIT_JOB_ID,
                        "28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        "testreceipt@something.com",
                    )
                except Exception as ex:
                    raise ex
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(
                        config.get_temp_folder(INIT_JOB_ID), ignore_errors=True
                    )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_partition_processing_with_multiple_zarr(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        Test the process_data_files function and submit three job index 1, 2, 3, this make sure
        we can process zarr file by writing to multiple partition and read it at the end, this will spot the edge case
        during two part aggregation
        """
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        # This uuid contains two dataset in the canned data
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_delayed_qc.zarr
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_derived_product.zarr
        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:
                    # Job 1, use different job id to avoid read same folder
                    process_data_files(
                        job_id_of_init="888",
                        job_index="1",
                        intermediate_output_folder=config.get_temp_folder("888"),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-07-01 00:00:00"),
                        end_date=pd.Timestamp("2011-07-31 23:59:59.999999999"),
                        multi_polygon=None,
                    )
                    # Job 2
                    process_data_files(
                        job_id_of_init="888",
                        job_index="2",
                        intermediate_output_folder=config.get_temp_folder("888"),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-08-01 00:00:00"),
                        end_date=pd.Timestamp("2011-08-15 23:59:59.999999999"),
                        multi_polygon=None,
                    )
                    process_data_files(
                        job_id_of_init="888",
                        job_index="3",
                        intermediate_output_folder=config.get_temp_folder("888"),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-08-16 00:00:00"),
                        end_date=pd.Timestamp("2011-09-01 00:00:00"),
                        multi_polygon=None,
                    )
                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    names = helper.list_s3_folders(
                        config.get_csv_bucket_name(),
                        f"{config.get_s3_temp_folder_name('888')}vessel_satellite_radiance_delayed_qc.zarr",
                    )
                    assert "part-1.zarr" in names, "part-1.zarr not exit!"
                    assert "part-2.zarr" in names, "part-2.zarr not exit!"
                    assert "part-3.zarr" in names, "part-3.zarr not exit!"

                    # This will aggregate to the same row count as above
                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name('888')}vessel_satellite_radiance_delayed_qc.zarr/part-*.zarr"
                    data = helper.read_multipart_zarr_from_s3(target_path)
                    assert (
                        len(data["TIME"]) == 158902
                    ), "file have enough data and same as single file"
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_zarr_to_netcdf_is_not_empty(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        We hit a case where this dataset result in empty netcdf, we want to make sure it
        does not happens
        """
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
                        uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                        keys=["radar_CoffsHarbour_wind_delayed_qc.zarr"],
                        start_date=pd.Timestamp("2012-03-01 00:00:00"),
                        end_date=pd.Timestamp("2012-04-30 23:59:59.999999999"),
                        multi_polygon=None,
                    )
                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    names = helper.list_s3_folders(
                        config.get_csv_bucket_name(),
                        f"{config.get_s3_temp_folder_name('888')}radar_CoffsHarbour_wind_delayed_qc.zarr",
                    )
                    assert "part-1.zarr" in names, "part-1.zarr not exit!"

                    # This will aggregate to the same row count as above
                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name('888')}radar_CoffsHarbour_wind_delayed_qc.zarr/part-*.zarr"
                    data = helper.read_multipart_zarr_from_s3(target_path)
                    assert (
                        len(data["TIME"]) == 1
                    ), "this dataset is an edge case and should only have one time entry"
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)
