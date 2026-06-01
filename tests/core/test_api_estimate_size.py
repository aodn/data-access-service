"""Unit tests for BaseAPI.estimate_dataset_size — zarr path (issue #8182, plan §5a).

The zarr branch reads xarray.Dataset.nbytes on a lazily-sliced dataset, so we
mock get_datasource to return a fake ZarrDataSource whose get_data returns a
small in-memory xarray.Dataset with known sizes.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

from data_access_service import API
from data_access_service.core.constants import (
    COMPRESSION_RATIO_NETCDF,
    COMPRESSION_RATIO_GEOTIFF,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
)


def _identity_map(uuid, key, columns):
    """Stand-in for API.map_column_names: echo the requested names unchanged."""
    return list(columns) if columns else columns


def _make_gridded_dataset(time=3, lat=4, lon=5, var_dtype="float32") -> xr.Dataset:
    """A gridded dataset: one (TIME, LAT, LON) numeric var + one non-gridded var."""
    times = pd.date_range("2020-01-01", periods=time)
    grid = np.zeros((time, lat, lon), dtype=var_dtype)
    return xr.Dataset(
        {
            # gridded numeric var -> eligible for GeoTIFF
            "sea_surface_temperature": (("TIME", "LATITUDE", "LONGITUDE"), grid),
            # non-gridded var (TIME only) -> must be excluded from GeoTIFF
            "quality_level": ("TIME", np.zeros(time, dtype="int8")),
        },
        coords={
            "TIME": times,
            "LATITUDE": np.arange(lat, dtype="float64"),
            "LONGITUDE": np.arange(lon, dtype="float64"),
        },
    )


UUID = "test-uuid"
KEY = "test-key"


def _make_dataset() -> xr.Dataset:
    """A tiny dataset: 5 time steps, two float64 variables."""
    times = pd.date_range("2020-01-01", periods=5)
    return xr.Dataset(
        {
            "TEMP": ("TIME", np.arange(5, dtype="float64")),
            "PSAL": ("TIME", np.arange(5, dtype="float64")),
        },
        coords={"TIME": times},
    )


def _api_with_zarr(dataset: xr.Dataset) -> tuple[API, MagicMock]:
    """Build an API whose get_datasource returns a mocked ZarrDataSource."""
    api = API()
    # spec=ZarrDataSource makes isinstance(mock, ZarrDataSource) return True.
    mock_ds = MagicMock(spec=ZarrDataSource)
    mock_ds.get_data.return_value = dataset
    api.get_datasource = MagicMock(return_value=mock_ds)
    return api, mock_ds


def test_zarr_estimate_basic():
    dataset = _make_dataset()
    api, mock_ds = _api_with_zarr(dataset)

    result = api.estimate_dataset_size(UUID, KEY, output_format="netcdf")

    assert result["uuid"] == UUID
    assert result["key"] == KEY
    assert result["format"] == "netcdf"
    # 5 time steps -> 5 rows.
    assert result["estimated_row_count"] == 5
    # .nbytes sums across all data vars + coords; no compute needed.
    assert result["estimated_uncompressed_bytes"] == dataset.nbytes
    assert result["estimated_output_bytes"] == int(
        dataset.nbytes * COMPRESSION_RATIO_NETCDF
    )
    assert result["is_estimate"] is True
    mock_ds.get_data.assert_called_once()


def test_geotiff_non_gridded_falls_back_to_flat_ratio():
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    # _make_dataset has no lat/lon dims -> no gridded vars -> flat-ratio fallback.
    api.map_column_names = MagicMock(side_effect=_identity_map)

    result = api.estimate_dataset_size(UUID, KEY, output_format="geotiff")

    assert result["format"] == "geotiff"
    assert result["estimated_output_bytes"] == int(
        dataset.nbytes * COMPRESSION_RATIO_GEOTIFF
    )
    assert "fell back to flat ratio" in result["notes"]


def test_geotiff_gridded_dimension_based():
    # 3 time x 4 lat x 5 lon, float32 (4 bytes/pixel); quality_level excluded.
    dataset = _make_gridded_dataset(time=3, lat=4, lon=5, var_dtype="float32")
    api, _ = _api_with_zarr(dataset)
    api.map_column_names = MagicMock(side_effect=_identity_map)

    result = api.estimate_dataset_size(UUID, KEY, output_format="geotiff")

    raw = 3 * 4 * 5 * 4  # n_time x lat x lon x bytes_per_pixel(float32)
    assert result["estimated_output_bytes"] == int(raw * GEOTIFF_ZIP_RATIO)
    # only the gridded var counts, not quality_level
    assert "1 gridded var(s)" in result["notes"]


def test_geotiff_ij_grid_uses_ij_sizes():
    # Curvilinear grid: dims are (TIME, I, J); lat/lon are 2D vars on (I, J).
    # The estimate should treat I/J as the grid dims (no coord conversion needed).
    times = pd.date_range("2020-01-01", periods=2)
    ii, jj = 4, 6
    ds = xr.Dataset(
        {
            "temp": (("TIME", "I", "J"), np.zeros((2, ii, jj), dtype="float32")),
            "LATITUDE": (("I", "J"), np.zeros((ii, jj), dtype="float64")),
            "LONGITUDE": (("I", "J"), np.zeros((ii, jj), dtype="float64")),
        },
        coords={"TIME": times},
    )
    api, _ = _api_with_zarr(ds)
    api.map_column_names = MagicMock(side_effect=_identity_map)

    result = api.estimate_dataset_size(UUID, KEY, output_format="geotiff")

    raw = 2 * ii * jj * 4  # n_time x I x J x float32
    assert result["estimated_output_bytes"] == int(raw * GEOTIFF_ZIP_RATIO)
    # temp is gridded on I/J; the 2D LATITUDE/LONGITUDE vars are not counted.
    assert "1 gridded var(s)" in result["notes"]
    assert "fell back" not in result["notes"]


def test_geotiff_integer_var_treated_as_float32():
    # int16 gridded var: rasterio casts ints to float32, so 4 bytes/pixel, not 2.
    dataset = _make_gridded_dataset(time=2, lat=3, lon=3, var_dtype="int16")
    api, _ = _api_with_zarr(dataset)
    api.map_column_names = MagicMock(side_effect=_identity_map)

    result = api.estimate_dataset_size(UUID, KEY, output_format="geotiff")

    raw = 2 * 3 * 3 * GEOTIFF_INT_PIXEL_BYTES
    assert result["estimated_output_bytes"] == int(raw * GEOTIFF_ZIP_RATIO)


def test_unsupported_format_raises():
    api, _ = _api_with_zarr(_make_dataset())
    with pytest.raises(ValueError, match="output_format must be one of"):
        api.estimate_dataset_size(UUID, KEY, output_format="zarr")


def test_dataset_not_found_returns_none():
    api = API()
    api.get_datasource = MagicMock(return_value=None)
    assert api.estimate_dataset_size(UUID, KEY) is None


def test_columns_subset_reduces_size():
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)
    # Pretend the requested column maps straight through to "TEMP".
    api.map_column_names = MagicMock(return_value=["TEMP"])

    result = api.estimate_dataset_size(UUID, KEY, columns=["TEMP"])

    # Only TEMP (+ TIME coord) should count, less than the full dataset.
    assert result["estimated_uncompressed_bytes"] < dataset.nbytes
    assert result["estimated_uncompressed_bytes"] == dataset[["TEMP"]].nbytes


def test_parquet_path_not_implemented_yet():
    from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

    api = API()
    api.get_datasource = MagicMock(return_value=MagicMock(spec=ParquetDataSource))
    with pytest.raises(NotImplementedError):
        api.estimate_dataset_size(UUID, KEY)


def test_date_range_clamped_to_extent_then_estimated():
    # With both bounds given, the request is clamped to the dataset's temporal
    # extent (trim_date_range_for_keys) before slicing - same as batch download.
    dataset = _make_dataset()  # TIME = 2020-01-01 .. 2020-01-05
    api, mock_ds = _api_with_zarr(dataset)
    api.get_temporal_extent = MagicMock(
        return_value=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05"))
    )

    result = api.estimate_dataset_size(
        UUID,
        KEY,
        date_start=pd.Timestamp("2020-01-02", tz="UTC"),
        date_end=pd.Timestamp("2020-01-04", tz="UTC"),
    )

    # Inside the extent -> a real estimate (not the empty one) and get_data ran.
    assert result["estimated_uncompressed_bytes"] == dataset.nbytes
    mock_ds.get_data.assert_called_once()
    # The slice passed to the lib must be tz-naive strings (xarray can't compare
    # tz-aware values against the naive time coordinate).
    passed_start, passed_end = mock_ds.get_data.call_args.args[:2]
    assert passed_start == "2020-01-02 00:00:00"
    assert "+" not in passed_start and "+" not in passed_end


def test_request_outside_extent_returns_empty_estimate():
    # Request entirely outside the data -> trim returns (None, None) -> zero-size
    # estimate, and get_data is never called.
    dataset = _make_dataset()
    api, mock_ds = _api_with_zarr(dataset)
    api.get_temporal_extent = MagicMock(
        return_value=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05"))
    )

    result = api.estimate_dataset_size(
        UUID,
        KEY,
        date_start=pd.Timestamp("1990-01-01", tz="UTC"),
        date_end=pd.Timestamp("1990-12-31", tz="UTC"),
    )

    assert result["estimated_row_count"] == 0
    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0
    assert result["is_estimate"] is True
    assert "outside" in result["notes"]
    mock_ds.get_data.assert_not_called()
