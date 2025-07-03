import shutil
import pytz
import pandas as pd
import pytest

from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
from data_access_service.core.AWSHelper import AWSHelper
from aodn_cloud_optimised.lib import DataQuery
from data_access_service import Config, init_log
from data_access_service.tasks.generate_csv_file import (
    trim_date_range,
    query_data,
    process_data_files,
)
from tests.batch.batch_test_consts import INIT_JOB_ID
from tests.core.test_with_s3 import TestWithS3, REGION


class TestGenerateCSVFile(TestWithS3):

    @pytest.fixture
    def mock_api(self):
        return MagicMock()

    @pytest.fixture
    def mock_aws(self):
        return MagicMock()

    @pytest.fixture(scope="class")
    def upload_test_case_to_s3(self, aws_clients, localstack, mock_boto3_client):
        s3_client, _ = aws_clients
        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample2",
        )

    def test_trim_date_range_within_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            pd.Timestamp(year=2020, month=1, day=1, tz=pytz.UTC),
            pd.Timestamp(year=2022, month=12, day=31, tz=pytz.UTC),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=pd.Timestamp(year=2021, month=1, day=1),
            requested_end_date=pd.Timestamp(year=2021, month=12, day=31),
        )
        assert start_date == pd.Timestamp(year=2021, month=1, day=1)
        assert end_date == pd.Timestamp(year=2021, month=12, day=31)

    def test_trim_date_range_out_of_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            pd.Timestamp(year=2020, month=1, day=1, tz=pytz.UTC),
            pd.Timestamp(year=2022, month=12, day=31, tz=pytz.UTC),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=pd.Timestamp(year=2019, month=1, day=1),
            requested_end_date=pd.Timestamp(year=2023, month=1, day=1),
        )
        assert start_date == pd.Timestamp(year=2020, month=1, day=1)
        assert end_date == pd.Timestamp(year=2022, month=12, day=31)

    @patch("data_access_service.tasks.generate_csv_file.API")
    def test_query_data_no_data(self, mock_api):
        mock_api.get_dataset_data.return_value = None
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            start_date=pd.Timestamp(year=2021, month=1, day=1),
            end_date=pd.Timestamp(year=2021, month=12, day=31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is None

    @patch("data_access_service.tasks.generate_csv_file.API")
    def test_query_data_with_data(self, mock_api):
        mock_api.get_dataset_data.return_value = MagicMock(empty=False)
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            start_date=pd.Timestamp(year=2021, month=1, day=1),
            end_date=pd.Timestamp(year=2021, month=12, day=31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is not None

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_generate_csv_with_zarr_single(
        self,
        setup,
        setup_resources,
        localstack,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        Test the process_data_files function by doing a single job index 10, this make sure
        we can process zarr file by writing to and single partition, that is part-10.zarr in this case
        """
        s3_client, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
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
                    assert len(data["TIME"]) == 167472, "file have enough data"
                finally:
                    TestWithS3.delete_object_in_s3(
                        s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
                    )
                    TestWithS3.delete_object_in_s3(
                        s3_client, Config.get_config().get_csv_bucket_name()
                    )
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(
                        config.get_temp_folder(INIT_JOB_ID), ignore_errors=True
                    )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_generate_csv_with_zarr_multiple(
        self,
        setup,
        setup_resources,
        localstack,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """
        Test the process_data_files function and submit three job index 1, 2, 3, this make sure
        we can process zarr file by writing to multiple partition and read it at the end, this will spot the edge case
        during two part aggregation
        """
        s3_client, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        helper = AWSHelper()

        # This uuid contains two dataset in the canned data
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_delayed_qc.zarr
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_derived_product.zarr
        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:
                    # Job 1
                    process_data_files(
                        job_id_of_init=INIT_JOB_ID,
                        job_index="1",
                        intermediate_output_folder=config.get_temp_folder(INIT_JOB_ID),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-07-01 00:00:00"),
                        end_date=pd.Timestamp("2011-07-31 23:59:59"),
                        multi_polygon=None,
                    )
                    # Job 2
                    process_data_files(
                        job_id_of_init=INIT_JOB_ID,
                        job_index="2",
                        intermediate_output_folder=config.get_temp_folder(INIT_JOB_ID),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-08-01 00:00:00"),
                        end_date=pd.Timestamp("2011-08-15 23:59:59"),
                        multi_polygon=None,
                    )
                    process_data_files(
                        job_id_of_init=INIT_JOB_ID,
                        job_index="3",
                        intermediate_output_folder=config.get_temp_folder(INIT_JOB_ID),
                        uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                        keys=["*"],
                        start_date=pd.Timestamp("2011-08-16 00:00:00"),
                        end_date=pd.Timestamp("2011-09-01 00:00:00"),
                        multi_polygon=None,
                    )
                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    names = helper.list_s3_folders(
                        config.get_csv_bucket_name(),
                        f"{config.get_s3_temp_folder_name(INIT_JOB_ID)}vessel_satellite_radiance_delayed_qc.zarr",
                    )
                    assert "part-1.zarr" in names, "part-1.zarr not exit!"
                    assert "part-2.zarr" in names, "part-2.zarr not exit!"
                    assert "part-3.zarr" in names, "part-3.zarr not exit!"

                    # This will aggregate to the same row count as above
                    target_path = f"s3://{config.get_csv_bucket_name()}/{config.get_s3_temp_folder_name(INIT_JOB_ID)}vessel_satellite_radiance_delayed_qc.zarr/part-*.zarr"
                    data = helper.read_multipart_zarr_from_s3(target_path)
                    assert len(data["TIME"]) == 167472, "file have enough data"
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    TestWithS3.delete_object_in_s3(
                        s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
                    )
                    TestWithS3.delete_object_in_s3(
                        s3_client, Config.get_config().get_csv_bucket_name()
                    )
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(
                        config.get_temp_folder(INIT_JOB_ID), ignore_errors=True
                    )
