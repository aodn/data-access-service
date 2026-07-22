import json
import shutil

import numpy as np
import pandas as pd
import pytest
import tempfile
from tests.conftest import subset_request_factory
import xarray

from pathlib import Path
from unittest.mock import patch
from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata
from data_access_service import API, init_log
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.batch.subsetting import prepare_data
from data_access_service.config.config import Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.batch.subsetting.tasks.data_collection import (
    collect_data_files,
)
from data_access_service.utils.date_time_utils import split_date_range
from tests.batch.batch_test_consts import PREPARATION_PARAMETERS, INIT_JOB_ID
from tests.core.test_with_s3 import TestWithS3, REGION


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
        self,
        aws_clients,
        setup_resources,
        upload_test_case_to_s3,
        subset_request_factory,
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

                assert len(folders) == 3
                assert folders[1] == "animal_acoustic_tracking_delayed_qc.parquet/"

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

                bucket_name = config.get_subsetting_bucket_name()
                response = s3_client.list_objects_v2(Bucket=bucket_name)

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range and 1 dataschema csv for the parquet dataset
                assert len(objects) == 2
                assert "999/temp/dataschema.json" in objects
                assert (
                    "999/temp/autonomous_underwater_vehicle.parquet/part-3/PARTITION_KEY=2010-11/part.0.parquet"
                    in objects
                )

                # Check if the files are compressed and uploaded correctly
                compressed_s3_key = "999/autonomous_underwater_vehicle.zip"

                # Create dummy subset_request
                subset_request = subset_request_factory(
                    uuid="test-dataset-uuid",
                    recipient="test@example.com",
                )

                collect_data_files(
                    master_job_id="999",
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

                assert (
                    len(names) == 2
                ), "contain single data file plus one table schema file"

                schema_files = [f for f in names if "dataschema.csv" in f]
                data_files = [
                    f for f in names if f.endswith(".csv") and "dataschema" not in f
                ]

                # Check values
                schema_df = pd.read_csv(f"/tmp/{schema_files[0]}", index_col=0)
                assert len(schema_df) > 0, "Schema should not be empty"

                # Expect a csv in this case so we can load it back with panda
                csv = pd.read_csv(f"/tmp/{data_files[0]}")
                assert len(csv) == 16703, f"Expected 16703 rows, got {len(csv)}"

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
    def test_parquet_with_date32_type(
        self,
        aws_clients,
        setup_resources,
        upload_test_case_to_s3,
        subset_request_factory,
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

                assert len(folders) == 3
                assert folders[0] == "aggregated_seabird_nonqc.parquet/"

                # Verify DataQuery functionality
                aodn = DataQuery.GetAodn()
                metadata: Metadata = aodn.get_metadata()
                assert (
                    metadata.metadata_catalog().get("aggregated_seabird_nonqc.parquet")
                    is not None
                )

                # prepare data according to the test parameters
                api = API()
                api.initialize_metadata()

                # Clean any old S3 objects with prefix "994/" at the start
                bucket_name = config.get_subsetting_bucket_name()
                try:
                    objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="994/")
                    if "Contents" in objs:
                        for o in objs["Contents"]:
                            s3_client.delete_object(Bucket=bucket_name, Key=o["Key"])
                except Exception:
                    pass

                parameters = PREPARATION_PARAMETERS.copy()
                parameters[Parameters.MASTER_JOB_ID.value] = "994"
                parameters[Parameters.UUID.value] = (
                    "ec2c0ef9-3645-4ded-b617-c8297f6eb250"
                )
                parameters[Parameters.MULTI_POLYGON.value] = None
                parameters[Parameters.START_DATE.value] = "03-2025"
                parameters[Parameters.END_DATE.value] = "04-2025"
                parameters[Parameters.DATE_RANGES.value] = (
                    '{"0": ["2025-03-01 00:00:00.000000000", "2025-03-05 23:59:59.999999999"], "1": ["2025-03-06 00:00:00.000000000", "2025-03-10 23:59:59.999999999"], "2": ["2025-03-11 00:00:00.000000000", "2025-03-20 23:59:59.999999999"], "3": ["2025-03-20 00:00:00.000000000", "2025-03-25 23:59:59.999999999"], "4": ["2025-03-26 00:00:00.000000000", "2025-03-30 23:59:59.999999999"]}'
                )
                parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value] = "/tmp/tmp994"

                for i in range(5):
                    prepare_data(
                        api,
                        job_index=str(i),
                        parameters=parameters,
                    )

                bucket_name = config.get_subsetting_bucket_name()
                response = s3_client.list_objects_v2(Bucket=bucket_name)

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range and 1 dataschema csv for the parquet dataset
                assert "994/temp/dataschema.json" in objects
                assert (
                    "994/temp/aggregated_seabird_nonqc.parquet/part-1/PARTITION_KEY=2025-03/part.0.parquet"
                    in objects
                )

                # Check if the files are compressed and uploaded correctly
                compressed_s3_key = "994/aggregated_seabird_nonqc.zip"

                # Create dummy subset_request
                subset_request = subset_request_factory(
                    uuid="test-dataset-uuid",
                    recipient="test@example.com",
                )

                collect_data_files(
                    master_job_id="994",
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
                assert "aggregated_seabird_nonqc.zip" in call_kwargs["html_body"]

                # Download the zip file and check the content
                names: list[str] = helper.extract_zip_from_s3(
                    bucket_name, compressed_s3_key, "/tmp"
                )

                assert (
                    len(names) == 2
                ), "contain single data file plus one table schema file"

                schema_files = [f for f in names if "dataschema.csv" in f]
                data_files = [
                    f for f in names if f.endswith(".csv") and "dataschema" not in f
                ]

                # Check values
                schema_df = pd.read_csv(f"/tmp/{schema_files[0]}", index_col=0)
                assert len(schema_df) > 0, "Schema should not be empty"

                # Expect a csv in this case so we can load it back with panda
                csv = pd.read_csv(f"/tmp/{data_files[0]}")
                assert len(csv) == 93, f"Expected 93 rows, got {len(csv)}"

            except Exception as ex:
                raise ex
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder("994"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_parquet_with_date32_type_with_multipolygon(
        self,
        aws_clients,
        setup_resources,
        upload_test_case_to_s3,
        subset_request_factory,
    ):
        """
        aggregated_moorings dataset with date32 column type, testing spatial and temporal subsetting and the
        multipolygon have overlap. The code internally will handle it and we can compare result to make sure
        the lat/lng is within the boundary of the multipolygon.
        """
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        helper = AWSHelper()

        # Clean any old S3 objects with prefix "997/" at the start
        bucket_name = config.get_subsetting_bucket_name()
        try:
            objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="997/")
            if "Contents" in objs:
                for o in objs["Contents"]:
                    s3_client.delete_object(Bucket=bucket_name, Key=o["Key"])
        except Exception:
            pass

        # Mock query_unique_value for polygon partitions because aggregated_seabird_nonqc dataset
        # in the test resources is only partitioned by timestamp, not by polygon.
        orig_query_unique_value = DataQuery.query_unique_value

        def mock_query_unique_value(dataset, partition):
            if partition == "polygon":
                return {
                    "01030000000100000005000000000000000000000000000000008056C0000000000080664000000000008056C00000000000806640000000000080564000000000000000000000000000805640000000000000000000000000008056C0"
                }
            return orig_query_unique_value(dataset, partition)

        with (
            patch(
                "aodn_cloud_optimised.lib.DataQuery.query_unique_value",
                side_effect=mock_query_unique_value,
            ),
            patch.object(AWSHelper, "send_email") as mock_send_email,
        ):
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

                assert len(folders) == 3
                assert folders[0] == "aggregated_seabird_nonqc.parquet/"

                # Verify DataQuery functionality
                aodn = DataQuery.GetAodn()
                metadata: Metadata = aodn.get_metadata()
                assert (
                    metadata.metadata_catalog().get("aggregated_seabird_nonqc.parquet")
                    is not None
                )

                # prepare data according to the test parameters
                api = API()
                api.initialize_metadata()

                # An overlapping GeoJSON MultiPolygon (Box 1 and Box 2 overlap in the 146.0 to 147.0 longitude range)
                # Due to the cloudlib only support bounding box, what happen is after the union of the two box we will get a larger box, then it will
                # query data inside this larger box for the corresponding partitions.
                # The return result will further filtered by the original multipolygon using shapely to povide result within the polygons.
                overlapping_geojson_str = '{"type":"MultiPolygon","coordinates":[[[[144.0, -39.0], [144.0, -41.0], [147.0, -41.0], [147.0, -39.0], [144.0, -39.0]]], [[[146.0, -39.0], [146.0, -41.0], [149.0, -41.0], [149.0, -39.0], [146.0, -39.0]]]]}'

                parameters = PREPARATION_PARAMETERS.copy()
                parameters[Parameters.MASTER_JOB_ID.value] = "997"
                parameters[Parameters.UUID.value] = (
                    "ec2c0ef9-3645-4ded-b617-c8297f6eb250"
                )
                parameters[Parameters.MULTI_POLYGON.value] = overlapping_geojson_str
                parameters[Parameters.START_DATE.value] = "03-2025"
                parameters[Parameters.END_DATE.value] = "04-2025"
                parameters[Parameters.DATE_RANGES.value] = (
                    '{"0": ["2025-03-01 00:00:00.000000000", "2025-03-05 23:59:59.999999999"], "1": ["2025-03-06 00:00:00.000000000", "2025-03-10 23:59:59.999999999"], "2": ["2025-03-11 00:00:00.000000000", "2025-03-20 23:59:59.999999999"], "3": ["2025-03-20 00:00:00.000000000", "2025-03-25 23:59:59.999999999"], "4": ["2025-03-26 00:00:00.000000000", "2025-03-30 23:59:59.999999999"]}'
                )
                parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value] = "/tmp/tmp997"

                for i in range(5):
                    prepare_data(
                        api,
                        job_index=str(i),
                        parameters=parameters,
                    )

                bucket_name = config.get_subsetting_bucket_name()
                response = s3_client.list_objects_v2(Bucket=bucket_name)

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range and 1 dataschema csv for the parquet dataset
                assert "997/temp/dataschema.json" in objects
                assert (
                    "997/temp/aggregated_seabird_nonqc.parquet/part-2/PARTITION_KEY=2025-03/part.0.parquet"
                    in objects
                )

                # Check if the files are compressed and uploaded correctly
                compressed_s3_key = "997/aggregated_seabird_nonqc.zip"

                # Create dummy subset_request
                subset_request = subset_request_factory(
                    uuid="test-dataset-uuid",
                    recipient="test@example.com",
                )

                collect_data_files(
                    master_job_id="997",
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
                assert "aggregated_seabird_nonqc.zip" in call_kwargs["html_body"]

                # Download the zip file and check the content
                names: list[str] = helper.extract_zip_from_s3(
                    bucket_name, compressed_s3_key, "/tmp"
                )

                assert (
                    len(names) == 2
                ), "contain single data file plus one table schema file"

                schema_files = [f for f in names if "dataschema.csv" in f]
                data_files = [
                    f for f in names if f.endswith(".csv") and "dataschema" not in f
                ]

                # Check values
                schema_df = pd.read_csv(f"/tmp/{schema_files[0]}", index_col=0)
                assert len(schema_df) > 0, "Schema should not be empty"

                # Expect a csv in this case so we can load it back with panda
                csv = pd.read_csv(f"/tmp/{data_files[0]}")
                assert (
                    len(csv) == 66
                ), f"Expected 66 rows inside the overlapping polygon union, got {len(csv)}"

                # Validate all points are physically inside the unified overlapping polygons
                from shapely.geometry import (
                    Polygon as ShapelyPolygon,
                    Point as ShapelyPoint,
                )
                from shapely.ops import unary_union

                box1 = ShapelyPolygon(
                    [
                        [144.0, -39.0],
                        [144.0, -41.0],
                        [147.0, -41.0],
                        [147.0, -39.0],
                        [144.0, -39.0],
                    ]
                )
                box2 = ShapelyPolygon(
                    [
                        [146.0, -39.0],
                        [146.0, -41.0],
                        [149.0, -41.0],
                        [149.0, -39.0],
                        [146.0, -39.0],
                    ]
                )
                union_poly = unary_union([box1, box2])

                for index, row in csv.iterrows():
                    point = ShapelyPoint(
                        row["decimalLongitude"], row["decimalLatitude"]
                    )
                    assert union_poly.contains(point) or union_poly.touches(
                        point
                    ), f"Point {point} is outside the multipolygon union"

            except Exception as ex:
                raise ex
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder("997"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_non_specified_multi_polygon(
        self, aws_clients, setup_resources, upload_test_case_to_s3
    ):
        # Use a different master_job_id to avoid conflicts with other tests
        parameters = PREPARATION_PARAMETERS.copy()
        parameters[Parameters.MASTER_JOB_ID.value] = "996"
        parameters[Parameters.MULTI_POLYGON.value] = "non-specified"
        parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value] = "/tmp/tmp996"

        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        helper = AWSHelper()

        # Clean any old S3 objects with prefix "996/" at the start
        bucket_name = config.get_subsetting_bucket_name()
        try:
            objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="996/")
            if "Contents" in objs:
                for o in objs["Contents"]:
                    s3_client.delete_object(Bucket=bucket_name, Key=o["Key"])
        except Exception:
            pass

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

                assert len(folders) == 3
                assert folders[1] == "animal_acoustic_tracking_delayed_qc.parquet/"

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

                bucket_name = config.get_subsetting_bucket_name()
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix="996/temp/"
                )

                objects = []
                if "Contents" in response:
                    for obj in response["Contents"]:
                        objects.append(obj["Key"])
                #  in test parquet, only 1 data csv for the provided range and 1 dataschema.csv file for the parquet dataset
                assert len(objects) == 2
                assert (
                    "996/temp/autonomous_underwater_vehicle.parquet/part-3/PARTITION_KEY=2010-11/part.0.parquet"
                    in objects
                )
                assert "996/temp/dataschema.json" in objects

            except Exception as ex:
                raise ex
            finally:
                # Delete temp output folder as the name always same for testing
                shutil.rmtree(config.get_temp_folder("996"), ignore_errors=True)
