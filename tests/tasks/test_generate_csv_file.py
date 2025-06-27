import pytest

from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
from data_access_service.core.AWSHelper import AWSHelper
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config
from data_access_service.tasks.generate_csv_file import (
    trim_date_range,
    query_data,
    generate_zip_name,
    process_data_files,
)
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
            datetime(2020, 1, 1),
            datetime(2022, 12, 31),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=datetime(2021, 1, 1),
            requested_end_date=datetime(2021, 12, 31),
        )
        assert start_date == datetime(2021, 1, 1)
        assert end_date == datetime(2021, 12, 31)

    def test_trim_date_range_out_of_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            datetime(2020, 1, 1),
            datetime(2022, 12, 31),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=datetime(2019, 1, 1),
            requested_end_date=datetime(2023, 1, 1),
        )
        assert start_date == datetime(2020, 1, 1)
        assert end_date == datetime(2022, 12, 31)

    @patch("data_access_service.tasks.generate_csv_file.API")
    def test_query_data_no_data(self, mock_api):
        mock_api.get_dataset_data.return_value = None
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            start_date=datetime(2021, 1, 1),
            end_date=datetime(2021, 12, 31),
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
            start_date=datetime(2021, 1, 1),
            end_date=datetime(2021, 12, 31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is not None

    def test_generate_zip_name(self):
        uuid = "test-uuid"
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2021, 12, 31)
        zip_name = generate_zip_name(uuid, start_date, end_date)
        assert zip_name == "test-uuid_2021-01-01_2021-12-31"

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_generate_csv_with_zarr(
        self, setup_resources, localstack, aws_clients, upload_test_case_to_s3
    ):
        """Test subsetting with valid and invalid time ranges."""
        s3_client, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        # This uuid contains two dataset in the canned data
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_delayed_qc.zarr
        # uuid 28f8bfed-ca6a-472a-84e4-42563ce4df3f name vessel_satellite_radiance_derived_product.zarr
        with patch.object(AWSHelper, "send_email") as mock_send_email:
            try:
                test_job_id = "10"
                process_data_files(
                    test_job_id,
                    "28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                    ["*"],
                    datetime.strptime("2011-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    datetime.strptime("2011-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    None,
                )
                # Should not arrive this line as zarr cannot convert to CSV
                assert False
            except MemoryError as me:
                # Expect to throw exception as zarr file is too big and not make sense to
                # convert to CSV
                pass
