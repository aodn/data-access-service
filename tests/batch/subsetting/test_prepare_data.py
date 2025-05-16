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
from jinja2.bccache import Bucket
from testcontainers.localstack import LocalStackContainer

from data_access_service import API
from data_access_service.batch.subsetting import prepare_data, Parameters
from data_access_service.config.config import EnvType, Config, TestConfig
from botocore.config import Config as BotoConfig

from data_access_service.core.AWSClient import AWSClient
from data_access_service.utils.date_time_utils import get_monthly_date_range_array_from_
from tests.batch.batch_test_consts import AWS_TEST_REGION, INIT_JOB_ID, PREPARATION_PARAMETERS
from tests.utils import upload_to_s3, delete_object_in_s3


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
        region_name=AWS_TEST_REGION,
    )
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=AWS_TEST_REGION,
    )
    return s3_client, sqs_client

@pytest.fixture
def mock_boto3_client(monkeypatch, localstack):
    # Wrap boto3.client to use LocalStack endpoint
    original_client = boto3.client

    def wrapped_client(*args, **kwargs):
        if args and args[0] in ["s3", "ses"]:
            kwargs["endpoint_url"] = localstack.get_url()
            kwargs["region_name"] = AWS_TEST_REGION
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

def test_mock_list_object_v2(setup_resources, mock_boto3_client):
    s3 = mock_boto3_client("s3", config=BotoConfig(signature_version=UNSIGNED))

    # Upload folder to create test data
    upload_to_s3(
        s3,
        DataQuery.BUCKET_OPTIMISED_DEFAULT,
        Path(__file__).parent.parent.parent / "canned/s3_sample1",
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

    for i in range(5):
        prepare_data(master_job_id=INIT_JOB_ID, job_index=i, parameters=PREPARATION_PARAMETERS)

    response2 = s3.list_objects_v2(Bucket=Config.get_config().get_csv_bucket_name())

    objects = []
    if "Contents" in response2:
        for obj in response2["Contents"]:
            objects.append(obj["Key"])
    assert len(objects) == 4
    assert objects[0] == "init-job-id/temp/date_2014-03-01_2014-03-31_bbox_-180_-90_180_90.csv"
    assert objects[1] == "init-job-id/temp/date_2014-06-01_2014-06-30_bbox_-180_-90_180_90.csv"
    assert objects[2] == "init-job-id/temp/date_2014-11-01_2014-11-30_bbox_-180_-90_180_90.csv"
    assert objects[3] == "init-job-id/temp/date_2015-02-01_2015-04-30_bbox_-180_-90_180_90.csv"



    try:
        aodn = DataQuery.GetAodn()
        metadata: Metadata = aodn.get_metadata()

        assert (
            metadata.metadata_catalog().get("animal_acoustic_tracking_delayed_qc")
            is not None
        )
    finally:
        delete_object_in_s3(s3, DataQuery.BUCKET_OPTIMISED_DEFAULT)
        delete_object_in_s3(s3, Config.get_config().get_csv_bucket_name())

