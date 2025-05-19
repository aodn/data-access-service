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