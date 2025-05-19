from pathlib import Path

import pytest
import boto3
import os

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
        # Cleanup environment variable after tests
        os.environ.pop("PROFILE", None)

    @pytest.fixture(scope="class")
    def localstack(self):
        """Start LocalStack container with SQS and S3 services."""
        with LocalStackContainer(image="localstack/localstack:4.3.0") as localstack:
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

    @pytest.fixture(scope="function")
    def mock_boto3_client(self, monkeypatch, localstack):
        """Mock boto3.client to use LocalStack endpoint for S3 and SES."""
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
        yield wrapped_client

    @pytest.fixture(scope="class")
    def setup_resources(self, aws_clients):
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
    def upload_to_s3(s3_client, bucket_name, sub_folder):
        for root, _, files in os.walk(sub_folder):
            for file in files:
                local_path = Path(root) / file
                # Compute S3 key relative to TEST_DATA_FOLDER
                relative_path = local_path.relative_to(sub_folder)
                s3_key = f"{relative_path}"
                s3_client.upload_file(str(local_path), bucket_name, s3_key)
                print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")

    @staticmethod
    def delete_object_in_s3(s3_client, bucket_name):
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            print(f"Deleted {len(objects)} objects from s3://{bucket_name}")
