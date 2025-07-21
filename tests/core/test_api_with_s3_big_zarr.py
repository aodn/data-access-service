import pytest

from pathlib import Path
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config
from data_access_service.server import app, api_setup
from tests.core.test_with_s3 import TestWithS3, REGION
from data_access_service.core.AWSHelper import AWSHelper
from starlette.status import HTTP_200_OK
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch


class TestApiWithS3BigZarr(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, localstack, mock_boto3_client):
        s3_client, _, _ = aws_clients
        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample3",
        )

    @pytest.fixture(scope="function")
    def client(self, upload_test_case_to_s3):
        # Use LifespanManager to ensure lifespan events are triggered
        # Make sure file uploaded before init the app
        api = api_setup(app)
        return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

    @pytest.mark.asyncio
    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    async def test_fetch_data_correct(
        self, setup, localstack, aws_clients, setup_resources, client
    ):
        """
        The zarr load is of size 205M with multiple dimension on 1 month, if this work then
        our current data load per month works with server side event, so we are not comparing value here but
        just the number of count.
        :param setup:
        :param localstack:
        :param aws_clients:
        :param setup_resources:
        :param client:
        :return:
        """

        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            # Test with range, this dataset field is different, it called detection_timestamp
            param = {
                "start_date": "2008-08-01",
                "end_date": "2008-09-01",
                "columns": ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
                "f": "sse/json",
            }

            target = f"{config.BASE_URL}/data/2d496463-600c-465a-84a1-8a4ab76bd505/satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.zarr"

            response = await client.get(
                target,
                params=param,
                headers={
                    "X-API-Key": config.get_api_key(),
                    "Accept": "text/event-stream",
                },
            )

            assert response.status_code == HTTP_200_OK
            assert (
                response.headers["Content-Type"] == "text/event-stream; charset=utf-8"
            )

            events: int = 0
            async for _ in response.aiter_lines():
                events = events + 1

            assert events == 2019
