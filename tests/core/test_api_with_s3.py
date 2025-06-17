import pytest
import json

from pathlib import Path
from fastapi.testclient import TestClient
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config
from data_access_service.server import app, api_setup
from tests.core.test_with_s3 import TestWithS3, REGION
from data_access_service.core.AWSClient import AWSClient
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from unittest.mock import patch


class TestApiWithS3(TestWithS3):

    @pytest.fixture(scope="class")
    def upload_test_case_to_s3(self, aws_clients, localstack, mock_boto3_client):
        s3_client, _ = aws_clients
        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample2",
        )

    @pytest.fixture(scope="class")
    def client(self, upload_test_case_to_s3):
        # Use LifespanManager to ensure lifespan events are triggered
        # Make sure file uploaded before init the app
        api_setup(app)
        return TestClient(app)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_auth_fetch_data_correct(
        self, setup_resources, localstack, aws_clients, client
    ):
        """Test subsetting with valid and invalid time ranges."""
        s3_client, _ = aws_clients
        config = Config.get_config()

        with patch.object(AWSClient, "send_email") as mock_send_email:
            # Test with range, this dataset field is different, it called detection_timestamp
            param = {
                "start_date": "1999-11-07",
                "end_date": "2025-11-08",
                "columns": ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            }

            target = (
                config.BASE_URL
                + "/data/541d4f15-122a-443d-ab4e-2b5feb08d6a0/animal_acoustic_tracking_delayed_qc.parquet"
            )

            response = client.get(
                target,
                params=param,
            )
            # We have not set key so forbidden
            assert response.status_code == HTTP_403_FORBIDDEN

            response = client.get(
                target,
                params=param,
                headers={"X-API-KEY": "test"},
            )

            # The X-API-KEY has wrong key value
            assert response.status_code == HTTP_401_UNAUTHORIZED

            response = client.get(
                target,
                params=param,
                headers={"X-API-Key": config.get_api_key()},
            )

            # The X-API-KEY has typo, it should be X-API-Key
            assert response.status_code == HTTP_200_OK

            try:
                parsed = json.loads(response.content.decode("utf-8"))
                assert len(parsed) == 22, "Size not match"
                assert parsed[0] == {
                    "latitude": -27.7,
                    "longitude": 153.3,
                    "time": "2012-11-01",
                }, f"Unexpected JSON content: {parsed[0]}"
                assert parsed[21] == {
                    "latitude": -33.9,
                    "longitude": 151.3,
                    "time": "2014-07-01",
                }, f"Unexpected JSON content: {parsed[21]}"
            except json.JSONDecodeError as e:
                assert False, "Fail to parse to JSON"

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_fetch_data_correct_without_depth(
        self, setup_resources, localstack, aws_clients, client
    ):
        """Test subsetting with valid and invalid time ranges."""
        s3_client, _ = aws_clients
        config = Config.get_config()

        with patch.object(AWSClient, "send_email") as mock_send_email:
            # Test with range, this dataset field is different, dataset without DEPTH
            param = {
                "start_date": "2009-11-07",
                "end_date": "2025-11-08",
                "columns": ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            }

            response = client.get(
                config.BASE_URL
                + "/data/7e13b5f3-4a70-4e31-9e95-335efa491c5c/mooring_temperature_logger_delayed_qc.parquet",
                params=param,
                headers={"X-API-Key": config.get_api_key()},
            )

            # The X-API-KEY has typo, it should be X-API-Key
            assert response.status_code == HTTP_200_OK
            assert isinstance(response.content, bytes)

            # Read and process response body
            try:
                parsed = json.loads(response.content.decode("utf-8"))
                assert len(parsed) == 269052, "Number of record is incorrect"
                assert parsed[0] == {
                    "latitude": -36.2,
                    "longitude": 150.2,
                    "time": "2014-10-01",
                }, f"Unexpected JSON content: {parsed[0]}"
                assert parsed[269051] == {
                    "latitude": -36.2,
                    "longitude": 150.2,
                    "time": "2015-01-01",
                }, f"Unexpected JSON content: {parsed[269051]}"
            except json.JSONDecodeError as e:
                assert False, "Fail to parse to JSON"

    @pytest.fixture(scope="class")
    def cleanup(self, localstack):
        # Setup code
        localstack.stop()
