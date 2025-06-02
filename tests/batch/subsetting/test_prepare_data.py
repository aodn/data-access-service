import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata

from data_access_service.batch.subsetting import prepare_data
from data_access_service.config.config import Config
from data_access_service.tasks.data_collection import collect_data_files
from tests.batch.batch_test_consts import INIT_JOB_ID, PREPARATION_PARAMETERS
from tests.core.test_with_s3 import TestWithS3


class TestDataGeneration(TestWithS3):

    def test_data_preparation_and_collection(
            self, setup_resources, mock_boto3_client, monkeypatch
    ):
        s3 = mock_boto3_client("s3")

        mock_send_email = MagicMock()
        monkeypatch.setattr("data_access_service.tasks.data_collection.AWSClient.send_email", mock_send_email)


        try:
            # Upload folder to create test data
            TestWithS3.upload_to_s3(
                s3,
                DataQuery.BUCKET_OPTIMISED_DEFAULT,
                Path(__file__).parent.parent.parent / "canned/s3_sample1",
            )

            # List objects in S3
            response = s3.list_objects_v2(
                Bucket=DataQuery.BUCKET_OPTIMISED_DEFAULT,
                Prefix=DataQuery.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
                Delimiter="/",
            )

            folders = [
                prefix["Prefix"][len(prefix) - 1:]
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
                prepare_data(master_job_id=INIT_JOB_ID, job_index=i, parameters=PREPARATION_PARAMETERS)

            bucket_name = Config.get_config().get_csv_bucket_name()
            response = s3.list_objects_v2(Bucket=bucket_name)


            objects = []
            if "Contents" in response:
                for obj in response["Contents"]:
                    objects.append(obj["Key"])
            #  in test parquet, only 1 data csv for the provided range
            assert len(objects) == 1
            assert objects[0] == 'init-job-id/temp/date_2010-11-17_2010-11-30_bbox_-180_-90_180_90.csv'

            # Check if the files are compressed and uploaded correctly
            compressed_s3_key = "init-job-id/data.zip"
            collect_data_files(master_job_id="init-job-id", dataset_uuid="test-dataset-uuid", recipient="test@example.com")
            response2 = s3.list_objects_v2(Bucket=bucket_name, Prefix=compressed_s3_key)
            assert "Contents" in response2
            assert len(response2["Contents"]) == 1
            assert response2["Contents"][0]["Key"] == compressed_s3_key

            # Check if the email was sent correctly
            mock_send_email.assert_called_once_with(
                recipient="test@example.com",
                subject="Finish processing data file whose uuid is:  test-dataset-uuid",
                body_text="You can download the data file from the following link: https://test-bucket.s3.us-east-1.amazonaws.com/init-job-id/data.zip",
            )

            # Check if the uncompressed size matches the sum of the individual file sizes
            uncompressed_size = get_uncompressed_zip_size_from_s3(bucket_name, compressed_s3_key, s3)
            obj_sum_size = 0
            for obj in objects:
                obj_size = get_object_size_from_s3(bucket_name, obj, s3)
                if obj_size is not None:
                    obj_sum_size += obj_size

            assert uncompressed_size == obj_sum_size, "The size of the extracted files does not match the downloaded files."

        finally:
            TestWithS3.delete_object_in_s3(s3, DataQuery.BUCKET_OPTIMISED_DEFAULT)
            TestWithS3.delete_object_in_s3(s3, Config.get_config().get_csv_bucket_name())

def get_uncompressed_zip_size_from_s3(bucket_name, zip_key, s3_client):
    # Retrieve the ZIP file from S3
    zip_obj = s3_client.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = BytesIO(zip_obj['Body'].read())

    # Calculate the total uncompressed size
    total_size = 0
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            total_size += file_info.file_size
    return total_size

def get_object_size_from_s3(bucket_name, object_key, s3_client):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return response['ContentLength']
    except s3_client.exceptions.ClientError as e:
        raise ValueError(f"Error retrieving object size for {object_key}: {e}") from e