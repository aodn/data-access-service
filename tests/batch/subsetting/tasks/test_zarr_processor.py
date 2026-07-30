import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import xarray
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.batch.subsetting.tasks.zarr_processor import ZarrProcessor
from tests.core.test_with_s3 import TestWithS3, REGION

# A real-valued gridded store (SST analysis, lowercase lat/lon/time coords), so a
# download can be checked cell by cell - the radar store is all NaN.
RAMSSA_KEY = "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"
RAMSSA_UUID = "a4170ca8-0942-4d13-bdb8-ad4718ce14bb"


def _multi_polygon_of(*boxes) -> str:
    """GeoJSON MultiPolygon string, one rectangle ring per (west, south, east,
    north) box."""
    rings = [
        f"[[{w},{n}],[{w},{s}],[{e},{s}],[{e},{n}],[{w},{n}]]" for (w, s, e, n) in boxes
    ]
    return (
        '{"type":"MultiPolygon","coordinates":['
        + ",".join(f"[{r}]" for r in rings)
        + "]}"
    )


class TestSubsetZarr(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):

        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent.parent.parent / "canned/s3_sample2",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_zarr_processor(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
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
                        job_id="job_id_888",
                        subset_request=subset_request_factory(),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_subsetting_bucket_name(),
                        "",
                    )

                    assert (
                        "job_id_888/radar_CoffsHarbour_wind_delayed_qc.nc" in files
                    ), "didn't find expected output file"

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )

                        netcdf_xarray = xarray.open_dataset(temp_file_path)
                        assert (
                            netcdf_xarray.sizes["TIME"] == 1
                        ), f"TIME dimension size expected to be 1, but got {netcdf_xarray.dims['TIME']}"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    """
    This test is to cover a special case where the dimensions in ZARR are in descending order.
    Dimensions must be monotonic, but may be either ascending or descending.
    So the system should work for both ascending and descending dimensions.
    """

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_zarr_descending_dims(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
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
                        job_id="job_id_888",
                        subset_request=subset_request_factory(),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_subsetting_bucket_name(),
                        "",
                    )

                    assert (
                        "job_id_888/radar_CoffsHarbour_wind_delayed_qc.nc" in files
                    ), "didn't find expected output file"

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )

                        netcdf_xarray = xarray.open_dataset(temp_file_path)
                        assert (
                            netcdf_xarray.sizes["LATITUDE"] == 167
                        ), f"LATITUDE dimension size expected to be 167, but got {netcdf_xarray.dims['LATITUDE']}"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    def test_zarr_multi_bboxes(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
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
                        job_id="job_id_888",
                        subset_request=subset_request_factory(
                            multi_polygon='{"type":"MultiPolygon","coordinates":[[[[201.73699345083196,-47.61820213929325],[221.7761315086342,-47.61820213929325],[221.7761315086342,-38.939085797521166],[201.73699345083196,-38.939085797521166],[201.73699345083196,-47.61820213929325]]],[[[157.7915152538971,-32.07902332926048],[174.31501505594503,-32.07902332926048],[174.31501505594503,-15.428394281587785],[157.7915152538971,-15.428394281587785],[157.7915152538971,-32.07902332926048]]]]}',
                        ),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_subsetting_bucket_name(),
                        "",
                    )

                    assert (
                        "job_id_888/radar_CoffsHarbour_wind_delayed_qc.nc" in files
                    ), "didn't find expected output file"

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )
                except Exception as ex:
                    # Should not have any errors
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_multi_polygon_download_keeps_every_shape_and_blanks_the_rest(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
    ):
        """#8499 end to end: two disjoint drawn areas, one NetCDF.

        The old code merged the per-bbox slices with an outer join, so the file
        was the bounding ENVELOPE of both boxes and only held polygon 1's values.
        The file must now be the union grid: both areas' real values, everything
        else NaN, and no rows/columns from the gap between them.
        """
        config = Config.get_config()
        helper = AWSHelper()

        api = API()
        api.initialize_metadata()

        # Open ocean, so analysed_sst is finite in both areas (RAMSSA canned store)
        south = (150.0, -40.0, 152.0, -38.0)  # Tasman Sea
        north = (155.0, -30.0, 157.0, -28.0)  # off northern NSW

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with patch.object(AWSHelper, "send_email"):
                no_ext_key = RAMSSA_KEY.replace(".zarr", "")
                try:
                    ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=subset_request_factory(
                            uuid=RAMSSA_UUID,
                            keys=[RAMSSA_KEY],
                            start_date="2011-11-17",
                            end_date="2011-11-17",
                            multi_polygon=_multi_polygon_of(south, north),
                        ),
                    ).process()

                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )
                        result = xarray.open_dataset(temp_file_path)

                        lats, lons = result["lat"].values, result["lon"].values
                        # union grid: every axis position belongs to an area, and
                        # nothing from the gap between them is carried along
                        assert ((lats >= -40) & (lats <= -38)).sum() > 0
                        assert ((lats >= -30) & (lats <= -28)).sum() > 0
                        assert not ((lats > -38) & (lats < -30)).any(), "envelope rows"
                        assert not ((lons > 152) & (lons < 155)).any(), "envelope cols"

                        sst = result["analysed_sst"].isel(time=0)
                        in_south = sst.sel(lat=slice(-40, -38), lon=slice(150, 152))
                        in_north = sst.sel(lat=slice(-30, -28), lon=slice(155, 157))
                        # the "cross": area 1's lats x area 2's lons, asked for by
                        # neither, kept only because the grid is rectangular
                        cross = sst.sel(lat=slice(-40, -38), lon=slice(155, 157))

                        assert np.isfinite(in_south.values).all(), "area 1 data lost"
                        assert np.isfinite(in_north.values).all(), "area 2 data lost"
                        assert np.isnan(cross.values).all(), "cells nobody asked for"

                        # the surviving values are the store's own, not shifted
                        source = xarray.open_zarr(
                            Path(__file__).parent.parent.parent.parent
                            / "canned/s3_sample2"
                            / RAMSSA_KEY,
                            consolidated=False,
                        )["analysed_sst"].sel(time=result["time"].values[0])
                        np.testing.assert_allclose(
                            in_north.values,
                            source.sel(lat=in_north.lat, lon=in_north.lon).values,
                        )
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    def test_non_specified_multi_polygon(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
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
                        job_id="job_id_888",
                        subset_request=subset_request_factory(
                            multi_polygon="non-specified",
                        ),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_subsetting_bucket_name(),
                        "",
                    )

                    assert (
                        "job_id_888/radar_CoffsHarbour_wind_delayed_qc.nc" in files
                    ), "didn't find expected output file"

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )
                except Exception as ex:
                    # Should not have any errors
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    # for the dataset vessel_satellite_radiance_delayed_qc.zarr, the LATITUDE and LONGITUDE are not dimensions
    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_lat_lon_not_dim(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            # Patch fsspec to fix an issue were we cannot pass the storage_options correctly
            with patch.object(AWSHelper, "send_email") as mock_send_email:
                key = "vessel_satellite_radiance_delayed_qc.zarr"
                no_ext_key = key.replace(".zarr", "")
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=subset_request_factory(
                            uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                            keys=[key],
                            start_date="07-2011",
                            end_date="07-2011",
                        ),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_subsetting_bucket_name(),
                        "",
                    )

                    assert (
                        f"job_id_888/{no_ext_key}.nc" in files
                    ), "didn't find expected output file"

                    # use tempfile to download an object from s3
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        temp_file_path = Path(tmpdirname) / f"{no_ext_key}.nc"
                        helper.download_file_from_s3(
                            config.get_subsetting_bucket_name(),
                            f"job_id_888/{no_ext_key}.nc",
                            str(temp_file_path),
                        )

                        netcdf_xarray = xarray.open_dataset(temp_file_path)
                        assert (
                            netcdf_xarray.sizes["TIME"] == 4519
                        ), f"TIME dimension size expected to be 4519, but got {netcdf_xarray.dims['TIME']}"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)
