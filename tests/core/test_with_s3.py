import logging
from pathlib import Path
from typing import Any, Generator

import fsspec.config
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
from fsspec.core import get_fs_token_paths as original_get_fs_token_paths

from data_access_service.core.AWSHelper import AWSHelper

log = logging.getLogger(__name__)

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

    @pytest.fixture(autouse=True, scope="session")
    def setup(self):
        """Set environment variable for testing profile."""
        os.environ["PROFILE"] = EnvType.TESTING.value

    @pytest.fixture(autouse=True, scope="function")
    def clear_api_cache(self):
        """
        Placeholder fixture for test isolation.
        API._cached_metadata is an instance variable, so creating a new
        API instance automatically provides a clean cache. This fixture
        ensures tests use the api_instance fixture properly.
        """
        yield

    @pytest.fixture(scope="function")
    def api_instance(self):
        """
        Provide a fresh API instance for each test.
        Each test gets a new instance with
        empty _cached_metadata
        """
        from data_access_service.core.api import API

        log.debug("Creating fresh API instance")
        api = API()

        # Just to be extra safe, explicitly clear if it somehow got data because we have self._cached_metadata: Dict[str, Dict[str, Descriptor]] = dict() in code
        if hasattr(api, "_cached_metadata") and api._cached_metadata:
            log.warning("API instance had cached metadata, clearing it")
            api._cached_metadata.clear()

        api.initialize_metadata()
        log.debug(
            f"API initialized with {len(getattr(api, '_cached_metadata', {}))} cached items"
        )

        yield api

        # post-test cleanup
        if hasattr(API, "_instance"):
            API._instance = None

    @pytest.fixture(scope="class")
    def localstack(self, request) -> Generator[LocalStackContainer, Any, None]:
        """
        Start LocalStack container with SQS and S3 services.
        using with scope cause the call to localstack.stop() happen
        automatically
        """
        with LocalStackContainer(image="localstack/localstack:4.3.0") as localstack:
            localstack.start()
            time = wait_for_logs(localstack, "Ready.")
            log.info(
                f"Create localstack S3 at port {localstack.get_url()}, time = {time}"
            )

            # Tier down automatically
            yield localstack

            log.info(f"Close localstack S3 at port {localstack.get_url()}")

    @pytest.fixture(scope="class")
    def aws_clients(self, localstack):
        """Initialize AWS S3 and SQS clients pointing to LocalStack."""
        s3_client = boto3.client(
            "s3",
            endpoint_url=localstack.get_url(),
            aws_access_key_id=IntTestConfig.get_s3_test_key(),
            aws_secret_access_key=IntTestConfig.get_s3_secret(),
            region_name=REGION,
        )
        sqs_client = boto3.client(
            "sqs",
            endpoint_url=localstack.get_url(),
            aws_access_key_id=IntTestConfig.get_s3_test_key(),
            aws_secret_access_key=IntTestConfig.get_s3_secret(),
            region_name=REGION,
        )
        ses_client = boto3.client(
            "ses",
            endpoint_url=localstack.get_url(),
            aws_access_key_id=IntTestConfig.get_s3_test_key(),
            aws_secret_access_key=IntTestConfig.get_s3_secret(),
            region_name=REGION,
        )
        log.info(f"Bind S3 client to {localstack.get_url()}")
        yield s3_client, sqs_client, ses_client

    @pytest.fixture(scope="class")
    def mock_boto3_client(self, localstack):
        """
        Mock boto3.client and fsspec to use LocalStack endpoint for S3 and SES, this is needed
        so that the cloud optimized lib points to local stack s3 instead of the real cloud data.
        """
        monkeypatch = MonkeyPatch()  # Create manual MonkeyPatch instance
        localstack_endpoint = localstack.get_url()

        # patch get_s3_filesystem to use localstack
        import pyarrow.fs as fs

        def wrapped_get_s3_filesystem(s3_fs_opts=None):
            """Always return S3FileSystem configured for LocalStack"""
            log.debug(f"get_s3_filesystem called, returning LocalStack filesystem")
            return fs.S3FileSystem(
                endpoint_override=localstack_endpoint,
                region=REGION,
                anonymous=False,
                access_key="test",
                secret_key="test",
            )

        monkeypatch.setattr(
            "aodn_cloud_optimised.lib.DataQuery.get_s3_filesystem",
            wrapped_get_s3_filesystem,
        )

        original_client = boto3.client

        def wrapped_client(*args, **kwargs):
            if args and args[0] in ["s3", "ses", "batch"]:
                kwargs["endpoint_url"] = localstack.get_url()
                kwargs["region_name"] = REGION
                kwargs["config"] = BotoConfig(
                    signature_version=UNSIGNED, s3={"addressing_style": "path"}
                )
            return original_client(*args, **kwargs)

        monkeypatch.setattr(DataQuery.boto3, "client", wrapped_client)

        # Now patch get_mapper, which is use inside the ZarrDataSource lib. It different from
        # parquet which use the get_s3_filesystem(), so we need one more mock
        original_get_mapper = fsspec.get_mapper

        def wrapped_get_mapper(path, **kwargs):
            # Inject/update endpoint_url for LocalStack
            kwargs["endpoint_url"] = localstack.get_url()
            kwargs["anon"] = kwargs.get("anon", True)  # Preserve anon=True from lib

            # Call the REAL fsspec.get_mapper with modified kwargs
            real_mapper = original_get_mapper(path, **kwargs)
            return real_mapper

        monkeypatch.setattr(DataQuery.fsspec, "get_mapper", wrapped_get_mapper)

        monkeypatch.setattr(DataQuery, "ENDPOINT_URL", localstack.get_url())
        monkeypatch.setattr(DataQuery, "REGION", REGION)

        # Other test may have call this get_s3_filesystem() this function is cached and
        # may use different ENDPOINT_URL other than the one above
        # so we need to clear it now before the next call happens
        # this function is not applicable anymore so temperately commented this line
        # DataQuery.get_s3_filesystem.cache_clear()
        # Force a load so its cache value use the test value
        # DataQuery.get_s3_filesystem()

        yield wrapped_client
        monkeypatch.undo()

    @pytest.fixture(scope="class")
    def setup_resources(self, aws_clients):
        """Set up S3 buckets and SQS queue for testing."""
        s3_client, sqs_client, _ = aws_clients
        config: IntTestConfig = Config.get_config()
        config.set_s3_client(s3_client)

        # Create S3 buckets
        s3_client.create_bucket(Bucket=config.get_csv_bucket_name())
        s3_client.create_bucket(Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT)

        # Create SQS queue
        response = sqs_client.create_queue(QueueName="job-queue")
        queue_url = response["QueueUrl"]
        yield queue_url
        # Make sure it removed
        TestWithS3.delete_object_in_s3(s3_client, config.get_csv_bucket_name())
        TestWithS3.delete_object_in_s3(s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT)

    @pytest.fixture(scope="function")
    def mock_get_fs_token_paths(self, setup, setup_resources):
        """
        This mock is used to override a call to get_fs_token_paths in xarray
        where the storage_options is pass correctly causing test fail to scan
        the director of the localstack s3
        :param setup:
        :param setup_resources:
        :return:
        """
        helper = AWSHelper()

        def mock_fs(paths, mode="rb", **kwargs):
            # override this option
            kwargs["storage_options"] = helper.get_storage_options()
            return original_get_fs_token_paths(paths, mode, **kwargs)

        return mock_fs

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
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    # print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")
                except Exception as e:
                    print(
                        f"Failed to upload {local_path} to s3://{bucket_name}/{s3_key}: {e}"
                    )

    @staticmethod
    def delete_object_in_s3(s3_client, bucket_name):
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
