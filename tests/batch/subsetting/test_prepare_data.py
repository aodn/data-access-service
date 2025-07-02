import shutil
import zipfile

import pandas as pd
import pytest
import os

from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata

from data_access_service import init_log
from data_access_service.batch.subsetting import prepare_data
from data_access_service.config.config import Config, EnvType
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.tasks.data_collection import collect_data_files
from tests.batch.batch_test_consts import PREPARATION_PARAMETERS, INIT_JOB_ID
from tests.core.test_with_s3 import TestWithS3, REGION


class TestDataGeneration(TestWithS3):

    @pytest.fixture(autouse=True, scope="class")
    def setup(self):
        """Set environment variable for testing profile."""
        os.environ["PROFILE"] = EnvType.TESTING.value
        yield

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_parquet_preparation_and_collection(
        self, setup, setup_resources, aws_clients
    ):
        s3_client, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        log = init_log(config)
        helper = AWSHelper()

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            try:
                # Upload folder to create test data
                TestWithS3.upload_to_s3(
                    s3_client,
                    DataQuery.BUCKET_OPTIMISED_DEFAULT,
                    Path(__file__).parent.parent.parent / "canned/s3_sample1",
                )

                # List objects in S3
                response = s3_client.list_objects_v2(
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
                    metadata.metadata_catalog().get(
                        "animal_acoustic_tracking_delayed_qc.parquet"
                    )
                    is not None
                )

                # prepare data according to the test parameters
                for i in range(5):
                    prepare_data(job_index=str(i), parameters=PREPARATION_PARAMETERS)

                bucket_name = config.get_csv_bucket_name()
                response = s3_client.list_objects_v2(Bucket=bucket_name)

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range
                assert len(objects) == 1
                assert (
                    objects[0]
                    == "999/temp/autonomous_underwater_vehicle.parquet/part-3/PARTITION_KEY=2010-11-17/part.0.parquet"
                )

                # Check if the files are compressed and uploaded correctly
                compressed_s3_key = "999/autonomous_underwater_vehicle.zip"
                collect_data_files(
                    master_job_id="999",
                    dataset_uuid="test-dataset-uuid",
                    recipient="test@example.com",
                )
                response2 = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=compressed_s3_key
                )
                assert "Contents" in response2
                assert len(response2["Contents"]) == 1
                assert response2["Contents"][0]["Key"] == compressed_s3_key

                # Check if the email was sent correctly
                mock_send_email.assert_called_once_with(
                    recipient="test@example.com",
                    subject="Finish processing data file whose uuid is:  test-dataset-uuid",
                    body_text="You can download the data file from the following link: https://test-bucket.s3.us-east-1.amazonaws.com/999/autonomous_underwater_vehicle.zip",
                )

                # Download the zip file and check the content
                names: list[str] = helper.extract_zip_from_s3(
                    bucket_name, compressed_s3_key, "/tmp"
                )

                # Expect a csv in this case so we can load it back with panda
                assert len(names) == 1, "contain single file"

                # Check values
                csv = pd.read_csv(f"/tmp/{names[0]}")
                assert len(csv) == 16703, "line contain correct"

            except Exception as ex:
                log.error(ex)
                raise ex
            finally:
                TestWithS3.delete_object_in_s3(
                    s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
                )
                TestWithS3.delete_object_in_s3(
                    s3_client, Config.get_config().get_csv_bucket_name()
                )
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder(INIT_JOB_ID))
                os.remove(f"/tmp/{names[0]}")

    def test_data_preparation_without_index(self, setup, setup_resources, aws_clients):
        # Test the prepare_data function without a job index
        parameters = PREPARATION_PARAMETERS.copy()
        parameters["job_index"] = None
        parameters["start_date"] = "02-2010"
        parameters["end_date"] = "04-2011"

        s3_client, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            # Upload folder to create test data
            TestWithS3.upload_to_s3(
                s3_client,
                DataQuery.BUCKET_OPTIMISED_DEFAULT,
                Path(__file__).parent.parent.parent / "canned/s3_sample1",
            )

            try:
                prepare_data(job_index=None, parameters=parameters)
            except Exception as e:
                assert False, f"prepare_data raised an exception: {e}"
            finally:
                TestWithS3.delete_object_in_s3(
                    s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT
                )
                TestWithS3.delete_object_in_s3(
                    s3_client, Config.get_config().get_csv_bucket_name()
                )
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder(INIT_JOB_ID), ignore_errors=True)


def get_object_size_from_s3(bucket_name, object_key, s3_client):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return response["ContentLength"]
    except s3_client.exceptions.ClientError as e:
        raise ValueError(f"Error retrieving object size for {object_key}: {e}") from e
