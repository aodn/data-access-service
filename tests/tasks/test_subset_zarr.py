import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.tasks.subset_zarr import ZarrProcessor
from tests.core.test_with_s3 import TestWithS3, REGION


class TestSubsetZarr(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):

        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent / "canned/s3_sample2",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_new_zarr_subset(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                try:
                    zarr_processor = ZarrProcessor(
                        uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                        job_id="job_id_888",
                        keys=["radar_CoffsHarbour_wind_delayed_qc.zarr"],
                        start_date_str="03-2012",
                        end_date_str="04-2012",
                        multi_polygon='{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}',
                        recipient="example@@test.com",
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_csv_bucket_name(),
                        "",
                    )

                    assert (
                        "job_id_888/radar_CoffsHarbour_wind_delayed_qc.nc" in files
                    ), "didn't find expected output file"
                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)
