import shutil
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
import rasterio
import xarray
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.batch.subsetting_helper import get_subset_request
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.tasks.subset_zarr import ZarrProcessor
from tests.core.test_with_s3 import TestWithS3, REGION


_DEFAULT_MULTI_POLYGON = '{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}'
_DEFAULT_CITATION = "Cite data as: Mazor, T., Watermeyer, K., Hobley, T., Grinter, V., Holden, R., MacDonald, K. and Ferns, L. (2023). Statewide Marine Habitat Map."


def _build_request(
    uuid: str,
    keys: list[str],
    start_date_str: str = "03-2012",
    end_date_str: str = "04-2012",
    multi_polygon: str = _DEFAULT_MULTI_POLYGON,
    recipient: str = "example@test.com",
    collection_title: str = "Test Ocean Data Collection",
    full_metadata_link: str = "https://metadata.imas.utas.edu.au/.../test-uuid-123",
    suggested_citation: str = _DEFAULT_CITATION,
    output_format: str = "netcdf",
) -> SubsetRequest:
    return get_subset_request(
        {
            "uuid": uuid,
            "key": ",".join(keys),
            "start_date": start_date_str,
            "end_date": end_date_str,
            "multi_polygon": multi_polygon,
            "recipient": recipient,
            "collection_title": collection_title,
            "full_metadata_link": full_metadata_link,
            "suggested_citation": suggested_citation,
            "output_format": output_format,
        }
    )


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
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                            keys=[key],
                        ),
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
                        subset_request=_build_request(
                            uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                            keys=[key],
                        ),
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
                subset_request = subset_request_factory()
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                            keys=[key],
                            multi_polygon='{"type":"MultiPolygon","coordinates":[[[[201.73699345083196,-47.61820213929325],[221.7761315086342,-47.61820213929325],[221.7761315086342,-38.939085797521166],[201.73699345083196,-38.939085797521166],[201.73699345083196,-47.61820213929325]]],[[[157.7915152538971,-32.07902332926048],[174.31501505594503,-32.07902332926048],[174.31501505594503,-15.428394281587785],[157.7915152538971,-15.428394281587785],[157.7915152538971,-32.07902332926048]]]]}',
                        ),
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
                except Exception as ex:
                    # Should not have any errors
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    def test_non_specified_multi_polygon(
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
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                            keys=[key],
                            multi_polygon="non-specified",
                        ),
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
                except Exception as ex:
                    # Should not have any errors
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_zarr_processor_geotiff_output(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """GeoTIFF export should produce a single ZIP with all TIF files.
        TIF naming follows {dataset}_{variable}_{YYYY-MM-DD}.tif convention."""
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        helper = AWSHelper()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with patch.object(AWSHelper, "send_email") as mock_send_email:

                key = "radar_CoffsHarbour_wind_delayed_qc.zarr"
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
                            keys=[key],
                            suggested_citation="Cite data as: Test Citation.",
                            output_format="geotiff",
                        ),
                    )

                    zarr_processor.process()

                    # GeoTIFF output should produce ZIP files, not .nc
                    files = helper.list_all_s3_objects(
                        config.get_csv_bucket_name(),
                        "",
                    )

                    # Should have exactly one geotiff ZIP (all TIFs bundled together)
                    zip_files = [
                        f for f in files if f.endswith(".zip") and "geotiff" in f
                    ]
                    assert (
                        len(zip_files) == 1
                    ), f"Expected exactly 1 geotiff ZIP, got: {zip_files}"
                    assert zip_files[0].endswith(
                        "_geotiff.zip"
                    ), f"Expected {{dataset}}_geotiff.zip, got: {zip_files[0]}"

                    # Download and verify the ZIP contents
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        zip_s3_key = zip_files[0]
                        local_zip = Path(tmpdirname) / "output.zip"
                        helper.download_file_from_s3(
                            config.get_csv_bucket_name(),
                            zip_s3_key,
                            str(local_zip),
                        )

                        # Verify it's a valid ZIP containing .tif files
                        with zipfile.ZipFile(local_zip, "r") as zf:
                            tif_names = [n for n in zf.namelist() if n.endswith(".tif")]
                            assert (
                                len(tif_names) > 0
                            ), f"ZIP should contain .tif files, got: {zf.namelist()}"

                            # Verify zip structure: {variable}/{dataset}_{variable}_{date}.tif
                            dataset_base = key.replace(".zarr", "")
                            for tif_name in tif_names:
                                assert (
                                    "/" in tif_name
                                ), f"TIF should be in a variable subfolder, got: {tif_name}"
                                filename = tif_name.split("/")[-1]
                                assert filename.startswith(
                                    dataset_base
                                ), f"TIF name should start with '{dataset_base}', got: {tif_name}"
                                assert filename.endswith(
                                    ".tif"
                                ), f"Expected .tif extension, got: {tif_name}"

                            # Extract and verify a .tif is a valid GeoTIFF with correct CRS
                            zf.extractall(tmpdirname)
                            tif_path = Path(tmpdirname) / tif_names[0]
                            with rasterio.open(tif_path) as src:
                                assert src.crs is not None, "GeoTIFF should have a CRS"
                                assert (
                                    src.crs.to_epsg() == 4326
                                ), f"Expected EPSG:4326, got {src.crs}"
                                assert (
                                    src.width > 0 and src.height > 0
                                ), "GeoTIFF should have valid dimensions"

                except Exception as ex:
                    assert False, f"{ex}"
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_geotiff_non_gridded_raises_error(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
    ):
        """GeoTIFF export should raise ValueError for datasets without
        gridded numeric variables (e.g. vessel radiance with TIME/WAVELENGTH dims)."""
        s3_client, _, _ = aws_clients
        config = Config.get_config()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with patch.object(AWSHelper, "send_email") as mock_send_email:

                key = "vessel_satellite_radiance_delayed_qc.zarr"
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                            keys=[key],
                            start_date_str="07-2011",
                            end_date_str="07-2011",
                            suggested_citation="Cite data as: Test Citation.",
                            output_format="geotiff",
                        ),
                    )

                    with pytest.raises(
                        ValueError, match="No gridded numeric variables"
                    ):
                        zarr_processor.process()

                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    # for the dataset vessel_satellite_radiance_delayed_qc.zarr, the LATITUDE and LONGITUDE are not dimensions
    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_lat_lon_not_dim(
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
                key = "vessel_satellite_radiance_delayed_qc.zarr"
                no_ext_key = key.replace(".zarr", "")
                try:
                    zarr_processor = ZarrProcessor(
                        api,
                        job_id="job_id_888",
                        subset_request=_build_request(
                            uuid="28f8bfed-ca6a-472a-84e4-42563ce4df3f",
                            keys=[key],
                            start_date_str="07-2011",
                            end_date_str="07-2011",
                        ),
                    )

                    zarr_processor.process()

                    # This is a zarr file, we should be able to read the result from S3, and have part-1, part2 and part-3
                    files = helper.list_all_s3_objects(
                        config.get_csv_bucket_name(),
                        "",
                    )

                    assert (
                        f"job_id_888/{no_ext_key}.nc" in files
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
                            netcdf_xarray.sizes["TIME"] == 4519
                        ), f"TIME dimension size expected to be 4519, but got {netcdf_xarray.dims['TIME']}"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    def test_convert_ij_dims_to_latlon(self):
        """
        Some radar datasets use (TIME, I, J) as dims instead of (TIME, LATITUDE, LONGITUDE).
        This test checks that we can convert them so the rest of the pipeline works.

        Before:  UCUR(TIME, I, J),  LATITUDE(I, J),  LONGITUDE(I, J)
        After:   UCUR(TIME, LATITUDE, LONGITUDE)
        """
        import numpy as np
        from unittest.mock import MagicMock

        # Build a fake radar-like dataset with I,J dims
        expected_lats = np.array([-30.0, -29.0, -28.0])
        expected_lons = np.array([110.0, 111.0, 112.0, 113.0])
        lat_2d, lon_2d = np.meshgrid(expected_lats, expected_lons, indexing="ij")
        original_data = np.random.rand(2, 3, 4)

        ds = xarray.Dataset(
            {
                "UCUR": (["TIME", "I", "J"], original_data),
                "LATITUDE": (["I", "J"], lat_2d),
                "LONGITUDE": (["I", "J"], lon_2d),
            },
            coords={"TIME": [0, 1]},
        )

        # Mock the processor (we only need log and dim name lookup)
        mock_processor = MagicMock(spec=ZarrProcessor)
        mock_processor.log = MagicMock()
        mock_processor._ZarrProcessor__get_dim_names.return_value = (
            "LATITUDE",
            "LONGITUDE",
            "TIME",
        )

        # Run the conversion
        converted = ZarrProcessor._ZarrProcessor__convert_ij_dims_to_latlon(
            mock_processor, ds, "test.zarr"
        )

        # I,J should be replaced by LATITUDE/LONGITUDE
        assert "I" not in converted.dims
        assert "LATITUDE" in converted.dims and "LONGITUDE" in converted.dims

        # Lat/lon values should be correct
        np.testing.assert_array_equal(converted.LATITUDE.values, expected_lats)
        np.testing.assert_array_equal(converted.LONGITUDE.values, expected_lons)

        # Data should not change
        np.testing.assert_array_equal(converted["UCUR"].values, original_data)
