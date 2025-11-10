import json
import shutil

import numpy as np
import pandas as pd
import pytest
import tempfile
import xarray

from pathlib import Path
from unittest.mock import patch
from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata
from data_access_service import API, init_log
from data_access_service.batch.batch_enums import Parameters
from data_access_service.batch.subsetting import prepare_data
from data_access_service.config.config import Config, EnvType
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.tasks.data_collection import collect_data_files
from data_access_service.utils.date_time_utils import split_date_range
from tests.batch.batch_test_consts import PREPARATION_PARAMETERS, INIT_JOB_ID
from tests.core.test_with_s3 import TestWithS3, REGION
from tests.utils.test_email_generator import create_dummy_subset_request


class TestDataGeneration(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        """
        This will call only once, so you should not delete any update in any test case
        :param mock_boto3_client:
        :param setup_resources:
        :param aws_clients:
        :return:
        """
        s3_client, _, _ = aws_clients
        # Upload test data
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent.parent / "canned/s3_sample1",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_parquet_preparation_and_collection(
        self, aws_clients, setup_resources, upload_test_case_to_s3
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        helper = AWSHelper()

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            try:
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
                api = API()
                api.initialize_metadata()
                for i in range(5):
                    prepare_data(
                        api, job_index=str(i), parameters=PREPARATION_PARAMETERS
                    )

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
                    == "999/temp/autonomous_underwater_vehicle.parquet/part-3/PARTITION_KEY=2010-11/part.0.parquet"
                )

                # Check if the files are compressed and uploaded correctly
                compressed_s3_key = "999/autonomous_underwater_vehicle.zip"

                # Create dummy subset_request
                subset_request = create_dummy_subset_request(
                    uuid="test-dataset-uuid",
                    keys=["*"],
                    start_date="2010-02-01",
                    end_date="2011-04-30",
                    recipient="test@example.com",
                )

                collect_data_files(
                    master_job_id="999",
                    dataset_uuid="test-dataset-uuid",
                    recipient="test@example.com",
                    subset_request=subset_request,
                )

                response2 = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=compressed_s3_key
                )
                assert "Contents" in response2
                assert len(response2["Contents"]) == 1
                assert response2["Contents"][0]["Key"] == compressed_s3_key

                # Check if the email was sent correctly
                mock_send_email.assert_called_once()
                call_kwargs = mock_send_email.call_args.kwargs
                assert call_kwargs["recipient"] == "test@example.com"
                assert "test-dataset-uuid" in call_kwargs["subject"]
                assert "html_body" in call_kwargs
                assert "autonomous_underwater_vehicle.zip" in call_kwargs["html_body"]

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
                raise ex
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder(INIT_JOB_ID), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_data_preparation_without_index(
        self, aws_clients, setup_resources, upload_test_case_to_s3
    ):
        # Test the prepare_data function without a job index
        parameters = PREPARATION_PARAMETERS.copy()

        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        api = API()
        api.initialize_metadata()

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            try:
                prepare_data(api, job_index=None, parameters=parameters)
            except Exception as e:
                assert False, f"prepare_data raised an exception: {e}"
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder(INIT_JOB_ID), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_data_preparation_with_lat_lon(
        self, aws_clients, setup_resources, upload_test_case_to_s3
    ):
        """
        Test with satellite data where lat/lon is of range [0,360]. The dataset use in the test is specially crafted
        that the lon is > 180., so any query < 180 will yield no result

        We always assume opi interface [-180,180] and this test verify the code automatically handle it.

        :param aws_clients: The aws client to local s3
        :param setup_resources: Depends, just make sure resource setup correctly
        :param upload_test_case_to_s3: Make sure data uploaded to local s3
        :return: Nothing
        """
        parameters = PREPARATION_PARAMETERS.copy()
        parameters[Parameters.UUID.value] = "a4170ca8-0942-4d13-bdb8-ad4718ce14bb"
        parameters[Parameters.KEY.value] = (
            "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"
        )
        parameters[Parameters.MASTER_JOB_ID.value] = "master"

        parameters[Parameters.DATE_RANGES.value] = json.dumps(
            split_date_range(pd.Timestamp("2012-10-16"), pd.Timestamp("2012-11-16"), 1)
        )

        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        log = init_log(config)

        api = API()
        api.initialize_metadata()

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            with tempfile.TemporaryDirectory() as temp_folder:
                parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value] = temp_folder

                try:
                    # First test, no bounding box so system will auto assign -180, 180. Internally if code correct
                    # will result in value found from range > 180 because this is satellite data where system should
                    # adjust -180, 180 to 0, 360 based on metadata's lon range in the dataset
                    prepare_data(api, job_index="0", parameters=parameters)

                    # Now open the file and inspect values
                    store = xarray.open_mfdataset(
                        paths=f"{temp_folder}/{parameters[Parameters.KEY.value]}/part-*.zarr",
                        engine="zarr",
                        combine="nested",
                        consolidated=False,  # Must be false as the file is not consolidated_metadata()
                        parallel=False,
                    )

                    assert (
                        store.lon.size > 0
                    ), "This dataset contains lon > 180, if code right we should get value with -180, 180 bounds"

                    u = np.unique(store.lon.values)
                    assert 182.25 in u, "Values greate than 182.25 found"
                    assert 190.0 in u, "Values greate than 190.0 found"

                except Exception as e:
                    assert False, f"prepare_data raised an exception: {e}"

            with tempfile.TemporaryDirectory() as temp_folder:
                parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value] = temp_folder

                try:
                    # Second test we specify a polygon range from -180, 0, it should translated to 0, 180, due to our crafted data
                    # we should yield no result as above shows data range is 180 above.
                    parameters[Parameters.MULTI_POLYGON.value] = json.dumps(
                        obj={
                            "type": "MultiPolygon",
                            "coordinates": [
                                # POLYGON 1: Northern Hemisphere (-180 to 0)
                                [
                                    [
                                        [-180, 90],  # NW
                                        [0, 90],  # NE
                                        [0, 0],  # SE
                                        [-180, 0],  # SW
                                        [-180, 90],  # Close
                                    ]
                                ],
                                # POLYGON 2: Southern Hemisphere (-180 to 0)
                                [
                                    [
                                        [-180, 0],  # NW
                                        [0, 0],  # NE
                                        [0, -90],  # SE
                                        [-180, -90],  # SW
                                        [-180, 0],  # Close
                                    ]
                                ],
                            ],
                        }
                    )
                    prepare_data(api, job_index="0", parameters=parameters)

                    # Now open the file and inspect values
                    store = xarray.open_mfdataset(
                        paths=f"{temp_folder}/{parameters[Parameters.KEY.value]}/part-*.zarr",
                        engine="zarr",
                        combine="nested",
                        consolidated=False,  # Must be false as the file is not consolidated_metadata()
                        parallel=False,
                    )
                    log.info(store)

                    assert (
                        store.lon.size == 0
                    ), "This dataset contains lon > 180, if code right we should not get value"

                except Exception as e:
                    assert False, f"prepare_data raised an exception: {e}"

                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(
                        config.get_temp_folder(INIT_JOB_ID), ignore_errors=True
                    )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_non_specified_multi_polygon(
        self, aws_clients, setup_resources, upload_test_case_to_s3
    ):
        # Use a different master_job_id to avoid conflicts with other tests
        parameters = PREPARATION_PARAMETERS.copy()
        parameters[Parameters.MASTER_JOB_ID.value] = "998"
        parameters[Parameters.MULTI_POLYGON.value] = "non-specified"

        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        helper = AWSHelper()

        with patch.object(AWSHelper, "send_email") as mock_send_email:
            try:
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
                api = API()
                api.initialize_metadata()
                for i in range(5):
                    prepare_data(api, job_index=str(i), parameters=parameters)

                bucket_name = config.get_csv_bucket_name()
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix="998/temp/"
                )

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range
                assert len(objects) == 1
                assert (
                    objects[0]
                    == "998/temp/autonomous_underwater_vehicle.parquet/part-3/PARTITION_KEY=2010-11/part.0.parquet"
                )

            except Exception as ex:
                raise ex
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder("998"), ignore_errors=True)
