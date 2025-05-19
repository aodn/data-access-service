from pathlib import Path
from unittest.mock import patch

from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata
from botocore import UNSIGNED
from data_access_service.batch.subsetting import execute, ParamField
from botocore.config import Config as BotoConfig
from data_access_service.core.AWSClient import AWSClient
from tests.core.test_with_s3 import TestWithS3, REGION


# A polygon covering the whole world
WORLD_POLYGON = """{
    "coordinates": [
        [
            [
                [
                    -180,
                    90
                ],
                [
                    -180,
                    -90
                ],
                [
                    180,
                    -90
                ],
                [
                    180,
                    90
                ],
                [
                    -180,
                    90
                ]
            ]
        ]
    ],
    "type": "MultiPolygon"
}"""


class TestSubsetting(TestWithS3):
    """Test class for subsetting functionality with LocalStack and S3."""

    def test_mock_list_object_v2(self, setup_resources, mock_boto3_client):
        """Verify that objects can be listed and DataQuery works with mock data."""
        s3 = mock_boto3_client("s3", config=BotoConfig(signature_version=UNSIGNED))

        # Upload test data
        TestWithS3.upload_to_s3(
            s3,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample1",
        )

        try:
            # List objects in S3
            response = s3.list_objects_v2(
                Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT,
                Prefix=DataQuery.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
                Delimiter="/",
            )

            folders = [
                prefix["Prefix"][len(prefix) - 1 :]
                for prefix in response.get("CommonPrefixes", [])
                if prefix["Prefix"].endswith(".parquet/")
            ]

            assert len(folders) == 2
            assert folders[0] == "animal_acoustic_tracking_delayed_qc.parquet/"

            # Verify DataQuery functionality
            aodn = DataQuery.GetAodn()
            metadata: Metadata = aodn.get_metadata()
            assert (
                metadata.metadata_catalog().get("animal_acoustic_tracking_delayed_qc")
                is not None
            )
        finally:
            TestWithS3.delete_object_in_s3(s3, DataQuery.BUCKET_OPTIMISED_DEFAULT)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_subsetting(
        self, localstack, aws_clients, setup_resources, mock_boto3_client
    ):
        """Test subsetting with valid and invalid time ranges."""
        s3_client, _ = aws_clients

        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample1",
        )

        with patch.object(AWSClient, "send_email") as mock_send_email:
            with patch(
                "aodn_cloud_optimised.lib.DataQuery.ENDPOINT_URL", localstack.get_url()
            ):
                try:
                    # Test with invalid time range
                    params = {
                        ParamField.UUID.value: "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                        ParamField.START_DATE.value: "2022-10-10",
                        ParamField.END_DATE.value: "2023-10-10",
                        ParamField.MULTI_POLYGON.value: WORLD_POLYGON,
                        ParamField.RECIPIENT.value: "noreply@testing.com",
                    }
                    execute("job_id", params)
                    mock_send_email.assert_called_once_with(
                        "noreply@testing.com",
                        "Error",
                        "No data found for selected conditions",
                    )

                    # Test with valid time range
                    params = {
                        ParamField.UUID.value: "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                        ParamField.START_DATE.value: "2010-01-01",
                        ParamField.END_DATE.value: "2010-12-01",
                        ParamField.MULTI_POLYGON.value: WORLD_POLYGON,
                        ParamField.RECIPIENT.value: "noreply@testing.com",
                    }
                    execute("job_id", params)
                    assert mock_send_email.call_count == 2
                    call_args = mock_send_email.call_args

                    assert call_args is not None, "send_email was not called"
                    assert call_args[0][0] == "noreply@testing.com", "Email matches"
                    assert (
                        call_args[0][1]
                        == "finish processing data file whose uuid is: af5d0ff9-bb9c-4b7c-a63c-854a630b6984"
                    ), "Subject match"
                    assert (
                        "You can download it. The download link is: https://test-bucket.s3.us-east-1.amazonaws.com/job_id.zip"
                        in call_args[0][2]
                    ), "Correct s3 path"
                finally:
                    TestWithS3.delete_object_in_s3(
                        s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
                    )
