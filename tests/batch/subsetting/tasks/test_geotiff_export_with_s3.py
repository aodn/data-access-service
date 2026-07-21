"""Integration tests for GeoTIFF output of the zarr conversion.

GeoTIFF is just another output format of ZarrProcessor (alongside netCDF), so
these drive the real pipeline end to end (subset request -> S3 -> download)
against LocalStack. They reuse an existing canned store from s3_sample2
(satellite_ghrsst_l4_ramssa, which has real SST values) - no new sample needed.
Requests are clipped to a small open-ocean box so the export stays tiny while the
data is genuinely non-NaN (so the data goals actually have teeth).

Each test maps to a goal from the download-service evaluation doc:
  - test_matches_requested_date_extent_projection : Goal 1 (date/extent/projection)
                                                    + Goal 2 (valid GeoTIFF -> QGIS-loadable)
  - test_matches_netcdf_original                  : Goal 3 (data == netCDF)
  - test_matches_cloud_optimised_source           : Goal 4 (data == cloud-optimised zarr)

Shared mock-patching and temp cleanup live in the `_env` fixture, so each test
body reads inline. Unit tests of the geotiff_export module (incl. the I/J 2D->1D
collapse) live in test_geotiff_export.py.
"""

import shutil
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
import xarray
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.batch.subsetting.tasks.zarr_processor import ZarrProcessor
from tests.core.test_with_s3 import TestWithS3, REGION

# A real-valued store from s3_sample2 (SST analysis, lowercase lat/lon/time coords,
# two days). Clipped to a small open-ocean box so the export is tiny but non-NaN.
SAMPLES = Path(__file__).parent.parent.parent.parent / "canned/s3_sample2"
KEY = "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"
UUID = "a4170ca8-0942-4d13-bdb8-ad4718ce14bb"
REQUESTED_DATE = "2011-11-17"
WEST, SOUTH, EAST, NORTH = 150.0, -40.0, 152.0, -38.0  # Tasman Sea: SST is finite here
JOB_ID = "job_id_888"


def _bbox_polygon(west, south, east, north):
    """GeoJSON MultiPolygon string (lon/lat ring) for a bounding box."""
    return (
        '{"type":"MultiPolygon","coordinates":[[[['
        f"[{west},{north}],[{west},{south}],[{east},{south}],"
        f"[{east},{north}],[{west},{north}]"
        "]]]]}"
    )


def _source_at(source, var, raster):
    """Source values at the GeoTIFF's own pixel centres, so the two align exactly
    regardless of how the pipeline clipped the bbox."""
    lons = [raster.xy(0, col)[0] for col in range(raster.width)]
    lats = [raster.xy(row, 0)[1] for row in range(raster.height)]
    return (
        source[var]
        .sel(time=REQUESTED_DATE)
        .squeeze()
        .sel(lat=lats, lon=lons, method="nearest")
        .values
    )


class TestGeotiffExportWithS3(TestWithS3):

    @pytest.fixture(autouse=True)
    def _env(self, mock_get_fs_token_paths):
        """Patch region/fsspec/email for every test; clean the temp folder after."""
        with (
            patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION),
            patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths),
            patch.object(AWSHelper, "send_email"),
        ):
            yield
        shutil.rmtree(Config.get_config().get_temp_folder("888"), ignore_errors=True)

    @pytest.fixture(scope="function")
    def upload_samples_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(s3_client, DataQuery.BUCKET_OPTIMISED_DEFAULT, SAMPLES)

    def test_matches_requested_date_extent_projection(
        self, aws_clients, upload_samples_to_s3, subset_request_factory
    ):
        """Goal 1 + 2: every downloaded TIF is named with the requested date, clipped
        to the requested bbox, is EPSG:4326, and opens as a valid raster (the
        QGIS-loadable proxy)."""
        config = Config.get_config()
        helper = AWSHelper()
        api = API()
        api.initialize_metadata()
        base = KEY.replace(".zarr", "")

        ZarrProcessor(
            api,
            job_id=JOB_ID,
            subset_request=subset_request_factory(
                uuid=UUID,
                keys=[KEY],
                start_date=REQUESTED_DATE,
                end_date=REQUESTED_DATE,
                output_format="geotiff",
                multi_polygon=_bbox_polygon(WEST, SOUTH, EAST, NORTH),
            ),
        ).process()

        files = helper.list_all_s3_objects(config.get_csv_bucket_name(), "")
        zip_keys = [f for f in files if f.endswith(f"{base}_geotiff.zip")]
        assert len(zip_keys) == 1, f"expected one geotiff ZIP, got {zip_keys}"

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = Path(tmp) / "output.zip"
            helper.download_file_from_s3(
                config.get_csv_bucket_name(), zip_keys[0], str(local_zip)
            )
            with zipfile.ZipFile(local_zip) as zf:
                tif_names = zf.namelist()
                zf.extractall(tmp)

            # Every gridded variable is exported (a dropped variable fails here).
            assert {n.split("/")[0] for n in tif_names} == {
                "analysed_sst",
                "analysis_error",
                "mask",
                "sea_ice_fraction",
            }
            for name in tif_names:
                var = name.split("/")[0]
                assert name == f"{var}/{base}_{var}_{REQUESTED_DATE}.tif"
                # Opening each TIF is also the Goal 2 check: a valid, QGIS-loadable raster.
                with rasterio.open(Path(tmp) / name) as raster:
                    assert raster.crs.to_epsg() == 4326
                    assert raster.count == 1
                    bounds = raster.bounds
                    # clipped to within the requested bbox (allow one cell of edge).
                    assert bounds.left >= WEST - 0.1 and bounds.right <= EAST + 0.1
                    assert bounds.bottom >= SOUTH - 0.1 and bounds.top <= NORTH + 0.1

    def test_matches_netcdf_original(
        self, aws_clients, upload_samples_to_s3, subset_request_factory
    ):
        """Goal 3: GeoTIFF and netCDF exports of the same request hold identical SST."""
        config = Config.get_config()
        helper = AWSHelper()
        api = API()
        api.initialize_metadata()
        base = KEY.replace(".zarr", "")

        for output_format in ("geotiff", "netcdf"):
            ZarrProcessor(
                api,
                job_id=JOB_ID,
                subset_request=subset_request_factory(
                    uuid=UUID,
                    keys=[KEY],
                    start_date=REQUESTED_DATE,
                    end_date=REQUESTED_DATE,
                    output_format=output_format,
                    multi_polygon=_bbox_polygon(WEST, SOUTH, EAST, NORTH),
                ),
            ).process()

        files = helper.list_all_s3_objects(config.get_csv_bucket_name(), "")
        zip_key = next(f for f in files if f.endswith(f"{base}_geotiff.zip"))
        nc_key = next(f for f in files if f.endswith(f"{base}.nc"))

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = Path(tmp) / "output.zip"
            helper.download_file_from_s3(
                config.get_csv_bucket_name(), zip_key, str(local_zip)
            )
            with zipfile.ZipFile(local_zip) as zf:
                zf.extractall(tmp)
            tif = Path(tmp) / f"analysed_sst/{base}_analysed_sst_{REQUESTED_DATE}.tif"
            with rasterio.open(tif) as r:
                geotiff_sst = r.read(1)

            local_nc = Path(tmp) / "output.nc"
            helper.download_file_from_s3(
                config.get_csv_bucket_name(), nc_key, str(local_nc)
            )
            netcdf_sst = (
                xarray.open_dataset(local_nc)["analysed_sst"]
                .sel(time=REQUESTED_DATE)
                .squeeze()
                .sortby("lat", ascending=False)  # GeoTIFF is north-up
                .values
            )
            np.testing.assert_array_equal(geotiff_sst, netcdf_sst)

    def test_matches_cloud_optimised_source(
        self, aws_clients, upload_samples_to_s3, subset_request_factory
    ):
        """Goal 4: every GeoTIFF band equals the source cloud-optimised zarr (RMSE=0)."""
        config = Config.get_config()
        helper = AWSHelper()
        api = API()
        api.initialize_metadata()
        base = KEY.replace(".zarr", "")

        ZarrProcessor(
            api,
            job_id=JOB_ID,
            subset_request=subset_request_factory(
                uuid=UUID,
                keys=[KEY],
                start_date=REQUESTED_DATE,
                end_date=REQUESTED_DATE,
                output_format="geotiff",
                multi_polygon=_bbox_polygon(WEST, SOUTH, EAST, NORTH),
            ),
        ).process()

        files = helper.list_all_s3_objects(config.get_csv_bucket_name(), "")
        zip_key = next(f for f in files if f.endswith(f"{base}_geotiff.zip"))
        source = xarray.open_zarr(SAMPLES / KEY)

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = Path(tmp) / "output.zip"
            helper.download_file_from_s3(
                config.get_csv_bucket_name(), zip_key, str(local_zip)
            )
            with zipfile.ZipFile(local_zip) as zf:
                tif_names = zf.namelist()
                zf.extractall(tmp)

            for name in tif_names:
                var = name.split("/")[0]
                with rasterio.open(Path(tmp) / name) as raster:
                    np.testing.assert_array_equal(
                        raster.read(1), _source_at(source, var, raster)
                    )
