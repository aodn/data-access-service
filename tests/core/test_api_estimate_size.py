"""Unit tests for the estimate-size path (zarr).

Two layers:
- ``size_estimation.estimate_single_key_size`` — per-key worker; the zarr
  branch slices ``ds.zarr_store`` with the SAME ``subset_zarr`` verb the batch
  download uses, then reads xarray.Dataset.nbytes on the lazy selection. We mock
  get_datasource to return a fake ZarrDataSource whose ``zarr_store`` is a small
  in-memory dataset, so the real slicing runs against it. It takes a
  ResolvedSubsetRequest (dates/keys/bboxes already resolved by
  utils.subset_request_resolver, the same code path the batch download uses).
- ``API.estimate_datasets_size`` — public multi-key aggregator (resolves the
  request via resolve_subset_request, sums per key, owns output_format validation).
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

from data_access_service import API
from data_access_service.core import api as core_api
from data_access_service.core.size_estimation import estimate_single_key_size
from data_access_service.core.constants import (
    COMPRESSION_RATIO_NETCDF,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
    GEOTIFF_CURVILINEAR_INFLATION,
)
from data_access_service.utils.subset_request_resolver import (
    ResolvedSubsetRequest,
    resolve_bboxes,
)


def _identity_map(uuid, key, columns):
    """Stand-in for API.map_column_names: echo the requested names unchanged.

    The test fixtures name their dims TIME/LATITUDE/LONGITUDE, matching the
    STR_*_UPPER_CASE constants, so an identity map resolves them correctly.
    """
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


def _resolved(
    keys=None,
    start=pd.Timestamp("2020-01-01", tz="UTC"),
    end=pd.Timestamp("2020-01-05 23:59:59", tz="UTC"),
    bboxes=(),
    columns=None,
) -> ResolvedSubsetRequest:
    """A ResolvedSubsetRequest as resolve_subset_request would produce for these tests."""
    return ResolvedSubsetRequest(
        uuid=UUID,
        keys=keys or [KEY],
        start_date=start,
        end_date=end,
        bboxes=list(bboxes),
        columns=columns,
    )


def _make_dataset() -> xr.Dataset:
    """A tiny dataset: 5 time steps, two float64 vars on TIME, plus LATITUDE and
    LONGITUDE dims so the shared subset_zarr can apply the spatial filter (real
    gridded zarr always carries these axes)."""
    times = pd.date_range("2020-01-01", periods=5)
    return xr.Dataset(
        {
            "TEMP": ("TIME", np.arange(5, dtype="float64")),
            "PSAL": ("TIME", np.arange(5, dtype="float64")),
        },
        coords={
            "TIME": times,
            "LATITUDE": np.array([25.0, 35.0]),
            "LONGITUDE": np.array([15.0, 25.0]),
        },
    )


def _api_with_zarr(dataset: xr.Dataset) -> tuple[API, MagicMock]:
    """Build an API whose get_datasource returns a mocked ZarrDataSource whose
    zarr_store is the given in-memory dataset. map_column_names echoes names, so
    subset_zarr resolves TIME/LATITUDE/LONGITUDE to the dataset's own dims."""
    api = API()
    # spec=ZarrDataSource makes isinstance(mock, ZarrDataSource) return True.
    mock_ds = MagicMock(spec=ZarrDataSource)
    mock_ds.zarr_store = dataset
    api.get_datasource = MagicMock(return_value=mock_ds)
    api.map_column_names = MagicMock(side_effect=_identity_map)
    return api, mock_ds


def test_zarr_estimate_basic():
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    # No bbox -> whole-globe slice covers the whole dataset -> full nbytes.
    result = estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf")

    assert result["uuid"] == UUID
    assert result["key"] == KEY
    assert result["format"] == "netcdf"
    assert result["estimated_uncompressed_bytes"] == dataset.nbytes
    assert result["estimated_output_bytes"] == int(
        dataset.nbytes * COMPRESSION_RATIO_NETCDF
    )


def test_geotiff_non_gridded_raises():
    # No variable is gridded on lat+lon, so the export can't produce a GeoTIFF
    # (build_geotiff_zip raises). The estimate mirrors that and stops instead of
    # promising a size for a download that would fail.
    dataset = _make_dataset()  # vars on TIME only -> no var gridded on lat+lon
    api, _ = _api_with_zarr(dataset)

    with pytest.raises(ValueError, match="no gridded numeric variables"):
        estimate_single_key_size(api, KEY, _resolved(), output_format="geotiff")


def test_geotiff_gridded_dimension_based():
    # 3 time x 4 lat x 5 lon, float32 (4 bytes/pixel); quality_level excluded.
    dataset = _make_gridded_dataset(time=3, lat=4, lon=5, var_dtype="float32")
    api, _ = _api_with_zarr(dataset)

    result = estimate_single_key_size(api, KEY, _resolved(), output_format="geotiff")

    raw = 3 * 4 * 5 * 4  # n_time x lat x lon x bytes_per_pixel(float32)
    # geotiff uncompressed is the raster bytes (not xarray nbytes); output = raw x zip
    assert result["estimated_uncompressed_bytes"] == raw
    assert result["estimated_output_bytes"] == int(raw * GEOTIFF_ZIP_RATIO)
    # only the gridded var counts, not quality_level
    assert "1 gridded var(s)" in result["notes"]


def test_geotiff_ij_grid_uses_ij_sizes():
    # Curvilinear grid: dims are (TIME, I, J), LAT/LON are 2D variables. The
    # spatial filter becomes a where() mask (kept lazy) and the estimate should
    # treat I/J as the grid dims (no coord conversion needed).
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

    result = estimate_single_key_size(api, KEY, _resolved(), output_format="geotiff")

    raw = 2 * ii * jj * 4  # n_time x I x J x float32
    # curvilinear (I/J) grid: inflated as an upper bound for the export warp
    inflated = int(raw * GEOTIFF_CURVILINEAR_INFLATION)
    assert result["estimated_uncompressed_bytes"] == inflated
    assert result["estimated_output_bytes"] == int(inflated * GEOTIFF_ZIP_RATIO)
    # temp is gridded on I/J; the 2D LATITUDE/LONGITUDE vars are not counted.
    assert "1 gridded var(s)" in result["notes"]
    assert "curvilinear grid" in result["notes"]
    assert "fell back" not in result["notes"]


def test_geotiff_integer_var_treated_as_float32():
    # int16 gridded var: rasterio casts ints to float32, so 4 bytes/pixel, not 2.
    dataset = _make_gridded_dataset(time=2, lat=3, lon=3, var_dtype="int16")
    api, _ = _api_with_zarr(dataset)

    result = estimate_single_key_size(api, KEY, _resolved(), output_format="geotiff")

    raw = 2 * 3 * 3 * GEOTIFF_INT_PIXEL_BYTES
    assert result["estimated_uncompressed_bytes"] == raw
    assert result["estimated_output_bytes"] == int(raw * GEOTIFF_ZIP_RATIO)


def test_unsupported_format_raises():
    # output_format validation lives in the public multi-key entry point.
    api, _ = _api_with_zarr(_make_dataset())
    with pytest.raises(ValueError, match="output_format must be one of"):
        api.estimate_datasets_size(UUID, keys=[KEY], output_format="zarr")


def test_dataset_not_found_returns_none():
    api = API()
    api.get_datasource = MagicMock(return_value=None)
    assert (
        estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf") is None
    )


def test_columns_not_applied_but_noted():
    # Column subsetting isn't implemented yet: the estimate ignores the requested
    # columns (covers the whole dataset) and says so in the notes. Once column
    # subsetting lands it will be handled in subset_zarr, not here.
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    result = estimate_single_key_size(
        api, KEY, _resolved(columns=["TEMP"]), output_format="netcdf"
    )

    assert result["estimated_uncompressed_bytes"] == dataset.nbytes
    assert "column subsetting not supported yet" in result["notes"]


def _single_polygon_geojson(lon_min, lat_min, lon_max, lat_max) -> str:
    """A GeoJSON MultiPolygon string holding one rectangular polygon."""
    ring = [
        [lon_min, lat_min],
        [lon_max, lat_min],
        [lon_max, lat_max],
        [lon_min, lat_max],
        [lon_min, lat_min],
    ]
    return (
        '{"type": "MultiPolygon", "coordinates": [['
        + str(ring).replace("'", "")
        + "]]}"
    )


def test_multi_polygon_single_slices_to_bbox():
    # A multi_polygon with one rectangle is resolved to its bbox via the same
    # resolve_bboxes (MultiPolygonHelper) the batch path uses, then actually
    # sliced. The bbox keeps only LATITUDE 35 (25 is below 30), so the estimate
    # is the sliced dataset's nbytes - strictly smaller than the whole dataset.
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    polygon = _single_polygon_geojson(lon_min=10, lat_min=30, lon_max=30, lat_max=40)
    result = estimate_single_key_size(
        api, KEY, _resolved(bboxes=resolve_bboxes(polygon)), output_format="netcdf"
    )

    # Oracle: apply the same time + spatial selection with xarray directly.
    oracle = dataset.sel(
        TIME=slice(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05 23:59:59")),
        LATITUDE=slice(30, 40),
        LONGITUDE=slice(10, 30),
    )
    assert result["estimated_uncompressed_bytes"] == oracle.nbytes
    assert result["estimated_uncompressed_bytes"] < dataset.nbytes


def test_no_spatial_filter_uses_whole_globe_bbox():
    # When there are no bboxes (no multi_polygon given), the whole-globe bbox is
    # used - the same slice the batch download uses
    # (ResolvedSubsetRequest.effective_bboxes) - so the whole dataset is counted.
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    result = estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf")

    assert result["estimated_uncompressed_bytes"] == dataset.nbytes


def test_multi_polygon_multiple_bboxes_summed():
    # Two polygons (both covering the tiny dataset) -> sliced per bbox and the
    # per-bbox .nbytes are SUMMED (overlaps counted twice = upper bound).
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    ring_a = [[10, 10], [40, 10], [40, 40], [10, 40], [10, 10]]
    ring_b = [[10, 10], [50, 10], [50, 50], [10, 50], [10, 10]]
    polygon = (
        '{"type": "MultiPolygon", "coordinates": ['
        + "["
        + str(ring_a).replace("'", "")
        + "],"
        + "["
        + str(ring_b).replace("'", "")
        + "]]}"
    )

    result = estimate_single_key_size(
        api, KEY, _resolved(bboxes=resolve_bboxes(polygon)), output_format="netcdf"
    )

    assert "summed 2 polygon bboxes" in result["notes"]
    # each bbox covers the whole dataset -> bytes are summed (2x)
    assert result["estimated_uncompressed_bytes"] == 2 * dataset.nbytes


def test_invalid_multi_polygon_raises():
    # A valid-JSON but non-MultiPolygon geometry -> TypeError from resolve_bboxes
    # (called by resolve_subset_request), which the route maps to a 400.
    bad = '{"type": "Point", "coordinates": [10, 20]}'
    with pytest.raises(TypeError):
        resolve_bboxes(bad)


def test_parquet_path_not_implemented_yet():
    from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

    api = API()
    api.get_datasource = MagicMock(return_value=MagicMock(spec=ParquetDataSource))
    with pytest.raises(NotImplementedError):
        estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf")


def test_date_range_clamped_to_extent_then_estimated():
    # With both bounds given, the request is clamped to the dataset's temporal
    # extent (trim_date_range_for_keys) before slicing, so the estimate reflects
    # the clamped 3-day window (2020-01-02..04), smaller than the full 5 days.
    dataset = _make_dataset()  # TIME = 2020-01-01 .. 2020-01-05
    api, _ = _api_with_zarr(dataset)
    api.get_temporal_extent = MagicMock(
        return_value=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05"))
    )

    result = estimate_single_key_size(
        api,
        KEY,
        _resolved(
            start=pd.Timestamp("2020-01-02", tz="UTC"),
            end=pd.Timestamp("2020-01-04", tz="UTC"),
        ),
        output_format="netcdf",
    )

    # Oracle: the same clamped time window applied directly (whole-globe space).
    oracle = dataset.sel(
        TIME=slice(pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-04 23:59:59"))
    )
    assert result["estimated_uncompressed_bytes"] == oracle.nbytes
    assert result["estimated_uncompressed_bytes"] < dataset.nbytes


def test_request_outside_extent_returns_empty_estimate():
    # Request entirely outside the data -> trim returns (None, None) -> zero-size
    # estimate, and the data is never sliced.
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)
    api.get_temporal_extent = MagicMock(
        return_value=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-05"))
    )

    result = estimate_single_key_size(
        api,
        KEY,
        _resolved(
            start=pd.Timestamp("1990-01-01", tz="UTC"),
            end=pd.Timestamp("1990-12-31", tz="UTC"),
        ),
        output_format="netcdf",
    )

    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0
    assert "outside" in result["notes"]


def test_resolved_without_data_returns_empty_estimate():
    # resolve_subset_request marks a no-overlap request with None dates (has_data
    # False); the worker must report zero size without touching the data.
    dataset = _make_dataset()
    api, _ = _api_with_zarr(dataset)

    result = estimate_single_key_size(
        api, KEY, _resolved(start=None, end=None), output_format="netcdf"
    )

    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0


def _single_result(key, unc, out):
    """A stand-in single-key estimate result, as estimate_single_key_size returns."""
    return {
        "uuid": UUID,
        "key": key,
        "format": "netcdf",
        "estimated_uncompressed_bytes": unc,
        "estimated_output_bytes": out,
        "notes": "",
    }


def test_multi_key_sums_and_keeps_breakdown(monkeypatch):
    api = API()
    mock_single = MagicMock(
        side_effect=[
            _single_result("a.zarr", 100, 40),
            _single_result("b.zarr", 200, 80),
        ]
    )
    # estimate_datasets_size calls the name imported into core.api, patch it there.
    monkeypatch.setattr(core_api, "estimate_single_key_size", mock_single)

    result = api.estimate_datasets_size(
        UUID, keys=["a.zarr", "b.zarr"], output_format="netcdf"
    )

    assert result["keys"] == ["a.zarr", "b.zarr"]
    assert result["estimated_uncompressed_bytes"] == 300
    assert result["estimated_output_bytes"] == 120
    assert "summed 2 keys" in result["notes"]
    # per-key breakdown is preserved
    assert [r["key"] for r in result["per_key"]] == ["a.zarr", "b.zarr"]


def test_star_expands_to_all_keys(monkeypatch):
    api = API()
    # "*" -> resolved from metadata, same as the batch download.
    api.get_mapped_meta_data = MagicMock(
        return_value={"a.zarr": object(), "b.zarr": object()}
    )
    mock_single = MagicMock(
        side_effect=[
            _single_result("a.zarr", 10, 4),
            _single_result("b.zarr", 10, 4),
        ]
    )
    monkeypatch.setattr(core_api, "estimate_single_key_size", mock_single)

    result = api.estimate_datasets_size(UUID, keys=["*"], output_format="netcdf")

    api.get_mapped_meta_data.assert_called_once_with(UUID)
    assert mock_single.call_count == 2
    assert result["keys"] == ["a.zarr", "b.zarr"]
    assert result["estimated_uncompressed_bytes"] == 20


def test_none_keys_means_all_keys(monkeypatch):
    api = API()
    api.get_mapped_meta_data = MagicMock(return_value={"only.zarr": object()})
    monkeypatch.setattr(
        core_api,
        "estimate_single_key_size",
        MagicMock(return_value=_single_result("only.zarr", 30, 12)),
    )

    result = api.estimate_datasets_size(UUID, keys=None, output_format="netcdf")

    api.get_mapped_meta_data.assert_called_once_with(UUID)
    assert result["keys"] == ["only.zarr"]


def test_missing_key_skipped_and_noted(monkeypatch):
    api = API()
    monkeypatch.setattr(
        core_api,
        "estimate_single_key_size",
        MagicMock(
            side_effect=[_single_result("a.zarr", 100, 40), None]  # b.zarr not found
        ),
    )

    result = api.estimate_datasets_size(
        UUID, keys=["a.zarr", "b.zarr"], output_format="netcdf"
    )

    assert result["keys"] == ["a.zarr"]
    assert result["estimated_uncompressed_bytes"] == 100
    assert "keys not found and skipped: ['b.zarr']" in result["notes"]


def test_all_keys_missing_returns_none(monkeypatch):
    api = API()
    monkeypatch.setattr(
        core_api, "estimate_single_key_size", MagicMock(return_value=None)
    )
    assert (
        api.estimate_datasets_size(UUID, keys=["nope.zarr"], output_format="netcdf")
        is None
    )
