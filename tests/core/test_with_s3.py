from pathlib import Path

import pytest
import boto3
import os

from _pytest.monkeypatch import MonkeyPatch
from testcontainers.core.waiting_utils import wait_for_logs

from data_access_service.config.config import EnvType, Config, IntTestConfig
from aodn_cloud_optimised.lib import DataQuery
from testcontainers.localstack import LocalStackContainer
from botocore.config import Config as BotoConfig
from botocore import UNSIGNED

# Default region for Localstack
REGION = "us-east-1"

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


class TestWithS3:

    @pytest.fixture(autouse=True, scope="class")
    def setup(self):
        """Set environment variable for testing profile."""
        os.environ["PROFILE"] = EnvType.TESTING.value
        yield

    @pytest.fixture(scope="class")
    def localstack(self):
        """Start LocalStack container with SQS and S3 services."""
        with LocalStackContainer(image="localstack/localstack:4.3.0") as localstack:
            wait_for_logs(localstack, "Ready")
            yield localstack

    @pytest.fixture(scope="class")
    def aws_clients(self, localstack):
        """Initialize AWS S3 and SQS clients pointing to LocalStack."""
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
        yield s3_client, sqs_client

    @pytest.fixture(scope="class")
    def mock_boto3_client(self, localstack):
        """Mock boto3.client to use LocalStack endpoint for S3 and SES."""
        monkeypatch = MonkeyPatch()  # Create manual MonkeyPatch instance
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
        monkeypatch.setattr(DataQuery, "ENDPOINT_URL", localstack.get_url())

        # Other test may have call this get_s3_filesystem() this function is cached and
        # may use different ENDPOINT_URL other than the one above
        # so we need to clear it now before the next call happens
        DataQuery.get_s3_filesystem.cache_clear()
        yield wrapped_client
        monkeypatch.undo()

    @pytest.fixture(scope="class")
    def setup_resources(self, aws_clients, mock_boto3_client):
        """Set up S3 buckets and SQS queue for testing."""
        s3_client, sqs_client = aws_clients
        config: IntTestConfig = Config.get_config()
        config.set_s3_client(s3_client)

        # Create S3 buckets
        s3_client.create_bucket(Bucket=config.get_csv_bucket_name())
        s3_client.create_bucket(Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT)

        # Create SQS queue
        response = sqs_client.create_queue(QueueName="job-queue")
        queue_url = response["QueueUrl"]
        yield queue_url

    # Util function to upload canned test data to localstack s3 or any s3
    @staticmethod
    def upload_to_s3(s3_client, bucket_name, folder):

        folder = Path(folder)  # Convert to Path object for robust path handling
        for root, dirs, files in os.walk(folder):
            for file in files:
                local_path = Path(root) / file
                # Compute S3 key relative to TEST_DATA_FOLDER
                relative_path = local_path.relative_to(folder)
                s3_key = str(relative_path).replace("\\", "/")
                s3_client.upload_file(str(local_path), bucket_name, s3_key)
                print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")

    @staticmethod
    def delete_object_in_s3(s3_client, bucket_name):
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            print(f"Deleted {len(objects)} objects from s3://{bucket_name}")
