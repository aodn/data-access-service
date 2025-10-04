import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import xarray
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
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
    def test_zarr_processor(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:

                key = "radar_CoffsHarbour_wind_delayed_qc.zarr"
                no_ext_key = key.replace(".zarr", "")
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                        job_id="job_id_888",
                        keys=[key],
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

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_csv_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )

                        netcdf_xarray = xarray.open_dataset(temp_file_path)
                        assert (
                            netcdf_xarray.dims["TIME"] == 1
                        ), f"TIME dimension size expected to be 1, but got {netcdf_xarray.dims['TIME']}"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)
