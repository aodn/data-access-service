from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import boto3
import os
from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from testcontainers.localstack import LocalStackContainer
from data_access_service.batch.subsetting import execute, Parameters
from data_access_service.config.config import EnvType, Config, TestConfig
from botocore.config import Config as BotoConfig

from data_access_service.core.AWSClient import AWSClient
from data_access_service.utils.date_time_utils import get_monthly_date_range_array_from_
from tests.utils import upload_to_s3, delete_object_in_s3

# default region for Localstack, other region not supported
REGION = "us-east-1"

# A polygon cover the whole world
world_polygon = """{
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


@pytest.fixture(scope="module")
def setup():
    os.environ["PROFILE"] = EnvType.TESTING.value


@pytest.fixture(scope="module")
def localstack(setup):
    # Start LocalStack with SQS and S3
    with LocalStackContainer(image="localstack/localstack:4.3.0") as localstack:
        yield localstack


@pytest.fixture(scope="module")
def aws_clients(localstack):
    # Initialize AWS clients pointing to LocalStack
    s3_client = boto3.client(
        "s3",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )
    return s3_client, sqs_client


# The DataQuery use default parameter to create S3 client, we need to override the values
# so that it calls boto3.client with other default value.
@pytest.fixture
def mock_boto3_client(monkeypatch, localstack):
    # Wrap boto3.client to use LocalStack endpoint
    original_client = boto3.client

    def wrapped_client(*args, **kwargs):
        if args and args[0] in ["s3", "ses"]:
            kwargs["endpoint_url"] = localstack.get_url()
            kwargs["region_name"] = REGION
            kwargs["config"] = BotoConfig(
                signature_version=UNSIGNED, s3={"addressing_style": "path"}
            )
        return original_client(*args, **kwargs)

    monkeypatch.setattr(DataQuery.boto3, "client", wrapped_client)
    return wrapped_client


@pytest.fixture(scope="module")
def setup_resources(aws_clients):
    s3_client, sqs_client = aws_clients

    # Overwrite with local stack s3 mock client
    config: TestConfig = Config.get_config()
    config.set_s3_client(s3_client)

    # Create S3 buckets
    s3_client.create_bucket(Bucket=config.get_csv_bucket_name())

    # Setup mock data for query
    s3_client.create_bucket(Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT)

    # Create SQS queue
    response = sqs_client.create_queue(QueueName="job-queue")
    queue_url = response["QueueUrl"]

    return queue_url


# Verify we can upload canned folder to local stack s3, then call the list function and loop the folder
# Finally we call the DataQuery.Aodn() and verify the basic function works before the next test, with the
# mock above, the DataQuery.GetAodn() will use the same mock_boto3_client
def test_mock_list_object_v2(setup_resources, mock_boto3_client):
    s3 = mock_boto3_client("s3", config=BotoConfig(signature_version=UNSIGNED))

    # Upload folder to create test data
    upload_to_s3(
        s3,
        DataQuery.BUCKET_OPTIMISED_DEFAULT,
        Path(__file__).parent.parent / "canned/s3_sample1",
    )
    response = s3.list_objects_v2(
        Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT,
        Prefix=DataQuery.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
        Delimiter="/",
    )

    folders = []
    for prefix in response.get("CommonPrefixes", []):
        folder_path = prefix["Prefix"]
        if folder_path.endswith(".parquet/"):
            folder_name = folder_path[len(prefix) - 1 :]
            folders.append(folder_name)

    assert len(folders) == 2
    assert folders[0] == "animal_acoustic_tracking_delayed_qc.parquet/"

    try:
        aodn = DataQuery.GetAodn()
        metadata: Metadata = aodn.get_metadata()

        assert (
            metadata.metadata_catalog().get("animal_acoustic_tracking_delayed_qc")
            is not None
        )
    finally:
        delete_object_in_s3(s3, DataQuery.BUCKET_OPTIMISED_DEFAULT)


# Test subsetting with valid and invalid time range
# mock_boto3_client is need to trigger mock set
@patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
def test_subsetting(localstack, aws_clients, setup_resources, mock_boto3_client):
    s3_client, sqs_client = aws_clients

    # Upload folder to create test data
    upload_to_s3(
        s3_client,
        DataQuery.BUCKET_OPTIMISED_DEFAULT,
        Path(__file__).parent.parent / "canned/s3_sample1",
    )

    with patch.object(AWSClient, "send_email") as mock_send_email:
        with patch(
            "aodn_cloud_optimised.lib.DataQuery.ENDPOINT_URL", localstack.get_url()
        ):
            try:
                # Simulate AWS Batch job by running the executor directly
                # Prepare the needed argument
                params = {
                    Parameters.UUID.value: "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                    Parameters.START_DATE.value: "2022-10-10",
                    Parameters.END_DATE.value: "2023-10-10",
                    Parameters.MULTI_POLYGON.value: world_polygon,
                    Parameters.RECIPIENT.value: "noreply@testing.com",
                }
                execute("job_id", params)
                mock_send_email.assert_called_once_with(
                    "noreply@testing.com",
                    "Error",
                    "No data found for selected conditions",
                )

                # Now try a valid time range
                params = {
                    Parameters.UUID.value: "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                    Parameters.START_DATE.value: "2010-01-01",
                    Parameters.END_DATE.value: "2010-12-01",
                    Parameters.MULTI_POLYGON.value: world_polygon,
                    Parameters.RECIPIENT.value: "noreply@testing.com",
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
                delete_object_in_s3(s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT)
