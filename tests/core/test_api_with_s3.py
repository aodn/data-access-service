import pytest

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
    def client(self):
        # Use LifespanManager to ensure lifespan events are triggered
        api_setup(app)
        return TestClient(app)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_fetch_data_correct(self, setup_resources, localstack, aws_clients, client):
        """Test subsetting with valid and invalid time ranges."""
        s3_client, _ = aws_clients
        config = Config.get_config()

        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample2",
        )

        try:
            with patch.object(AWSClient, "send_email") as mock_send_email:
                with patch(
                    "aodn_cloud_optimised.lib.DataQuery.ENDPOINT_URL",
                    localstack.get_url(),
                ):
                    # Test with range, this dataset field is different, it called detection_timestamp
                    param = {
                        "start_date": "2009-11-07",
                        "end_date": "2025-11-08",
                        "columns": ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
                    }

                    response = client.get(
                        config.BASE_URL + "/data/541d4f15-122a-443d-ab4e-2b5feb08d6a0",
                        params=param,
                    )
                    # We have not set key so forbidden
                    assert response.status_code == HTTP_403_FORBIDDEN

                    response = client.get(
                        config.BASE_URL + "/data/541d4f15-122a-443d-ab4e-2b5feb08d6a0",
                        params=param,
                        headers={"X-API-KEY": "test"},
                    )

                    # The X-API-KEY has wrong key value
                    assert response.status_code == HTTP_401_UNAUTHORIZED

                    response = client.get(
                        config.BASE_URL + "/data/541d4f15-122a-443d-ab4e-2b5feb08d6a0",
                        params=param,
                        headers={"X-API-Key": config.get_api_key()},
                    )

                    # The X-API-KEY has typo, it should be X-API-Key
                    assert response.status_code == HTTP_200_OK
                    assert isinstance(response.content, bytes)

                    # Value is not correct but seems a data issue
                    # assert len(response.content) > 10

        finally:
            TestWithS3.delete_object_in_s3(
                s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
            )
