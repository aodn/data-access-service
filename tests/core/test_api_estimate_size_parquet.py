"""Unit tests for the estimate-size path (parquet).

A parquet key is estimated only from the pre-built estimation index. These
tests put a small index parquet on local disk and read it with a real DuckDB
connection, so the SQL runs for real; only the S3 parts are stubbed.
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

import data_access_service.core.estimation_index as estimation_index
from data_access_service.core.estimation_index import (
    EstimationIndexUnavailableError,
    read_index_estimate,
    sidecar_extent_provider,
)
from data_access_service.core.size_estimation import estimate_single_key_size
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.estimation_types import (
    ESTIMATION_INDEX_VERSION,
    EstimationSidecarMetadata,
    schema_fingerprint,
)
from data_access_service.utils.subset_request_resolver import ResolvedSubsetRequest

UUID = "test-uuid"
KEY = "test.parquet"
COLUMNS = ["TEMP", "TIME", "LATITUDE", "LONGITUDE"]

START = pd.Timestamp("2020-01-01", tz="UTC")
END = pd.Timestamp("2020-01-05 23:59:59", tz="UTC")


class _LocalClient:
    """Stands in for EstimationDuckDBClient: same execute(), no S3 secret."""

    def __init__(self):
        self._conn = duckdb.connect()

    def execute(self, sql, params=None):
        return self._conn.execute(sql, params or [])

    def create_s3_secret(self, bucket):
        pass


def _meta(**overrides) -> EstimationSidecarMetadata:
    fields = dict(
        version=ESTIMATION_INDEX_VERSION,
        uuid=UUID,
        key=KEY,
        bin_size=1.0,
        bin_merge_factor=1,
        min_date=20200101,
        max_date=20200110,
        has_time=True,
        total_rows=35,
        csv_bytes_per_row=100.0,
        csv_header_bytes=50,
        zip_ratio=0.2,
        sample_rows=35,
        sample_files=1,
        null_position_rows=0,
        out_of_range_position_rows=0,
        null_time_rows=0,
        column_count=len(COLUMNS),
        schema_fingerprint=schema_fingerprint(COLUMNS),
        last_updated="2020-01-11T00:00:00Z",
    )
    fields.update(overrides)
    return EstimationSidecarMetadata(**fields)


def _api() -> MagicMock:
    api = MagicMock()
    api.get_dataset_variables.return_value = {UUID: {KEY: COLUMNS}}
    return api


@pytest.fixture
def index(tmp_path, monkeypatch):
    """A local index with 35 rows: 30 near 150E/35S, 5 near 10E/10N."""
    path = tmp_path / "index.parquet"
    pq.write_table(
        pa.table(
            {
                "d": pa.array([20200101, 20200105, 20200105], pa.int32()),
                "lat_bin": pa.array([-35, -35, 10], pa.int32()),
                "lon_bin": pa.array([150, 150, 10], pa.int32()),
                "c": pa.array([10, 20, 5], pa.int64()),
            }
        ),
        path,
    )
    monkeypatch.setattr(estimation_index, "index_s3_path", lambda uuid, key: str(path))
    monkeypatch.setattr(estimation_index, "load_sidecar", lambda uuid, key: _meta())
    estimation_index.set_duckdb_client(_LocalClient())
    yield path
    estimation_index.set_duckdb_client(None)


def _no_index(monkeypatch, meta=None):
    monkeypatch.setattr(estimation_index, "load_sidecar", lambda uuid, key: meta)


# --------------------------------------------------------------------------
# read_index_estimate - the only parquet estimator
# --------------------------------------------------------------------------


def test_estimate_is_rows_times_width_plus_header_times_zip_ratio(index):
    result = read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")

    assert result["uuid"] == UUID
    assert result["key"] == KEY
    assert result["format"] == "csv"
    assert result["estimated_uncompressed_bytes"] == 35 * 100 + 50
    assert result["estimated_output_bytes"] == int((35 * 100 + 50) * 0.2)
    assert "estimated from the pre-built index" in result["notes"]


def test_bbox_counts_only_the_cells_inside(index):
    bbox = BoundingBox(min_lon=149.0, min_lat=-36.0, max_lon=151.0, max_lat=-34.0)

    result = read_index_estimate(_api(), UUID, KEY, START, END, [bbox], "csv")

    assert result["estimated_uncompressed_bytes"] == 30 * 100 + 50
    assert "bbox upper bound" in result["notes"]


def test_no_matching_rows_is_zero_without_header(index):
    start = pd.Timestamp("2020-01-02", tz="UTC")
    end = pd.Timestamp("2020-01-03", tz="UTC")

    result = read_index_estimate(_api(), UUID, KEY, start, end, [], "csv")

    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0


def test_non_csv_format_is_skippable_value_error(index):
    """ValueError, so estimate_datasets_size skips the key as unsupported."""
    with pytest.raises(ValueError, match="only models the zipped-CSV download"):
        read_index_estimate(_api(), UUID, KEY, START, END, [], "netcdf")


# --------------------------------------------------------------------------
# No usable index -> the estimate fails with the reason
# --------------------------------------------------------------------------


def test_missing_index_raises(monkeypatch):
    _no_index(monkeypatch)

    with pytest.raises(EstimationIndexUnavailableError, match="has not been built"):
        read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")


def test_unknown_index_version_raises(monkeypatch):
    _no_index(monkeypatch, _meta(version=ESTIMATION_INDEX_VERSION + 1))

    with pytest.raises(EstimationIndexUnavailableError, match="is version"):
        read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")


def test_changed_columns_raise(monkeypatch):
    _no_index(monkeypatch, _meta(schema_fingerprint=schema_fingerprint(["TEMP"])))

    with pytest.raises(EstimationIndexUnavailableError, match="columns changed"):
        read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")


def test_unknown_fingerprint_is_not_a_change(monkeypatch, index):
    """An empty fingerprint means "cannot compare", so the index is still used."""
    monkeypatch.setattr(
        estimation_index,
        "load_sidecar",
        lambda uuid, key: _meta(schema_fingerprint=""),
    )

    result = read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")

    assert result["estimated_uncompressed_bytes"] > 0


def test_unreadable_index_raises(index, tmp_path, monkeypatch):
    monkeypatch.setattr(
        estimation_index,
        "index_s3_path",
        lambda uuid, key: str(tmp_path / "missing.parquet"),
    )

    with pytest.raises(EstimationIndexUnavailableError, match="could not be read"):
        read_index_estimate(_api(), UUID, KEY, START, END, [], "csv")


def test_unavailable_index_is_not_a_value_error():
    """estimate_datasets_size skips a key on ValueError; a missing index must
    fail the request instead."""
    assert not issubclass(EstimationIndexUnavailableError, ValueError)


# --------------------------------------------------------------------------
# sidecar_extent_provider - the date trim before the estimate
# --------------------------------------------------------------------------


def test_extent_comes_from_the_sidecar(index):
    api = _api()

    start, end = sidecar_extent_provider(api, "csv")(UUID, KEY)

    assert start == pd.Timestamp("2020-01-01", tz="UTC")
    assert end.date() == pd.Timestamp("2020-01-10").date()
    api.get_temporal_extent.assert_not_called()


def test_parquet_key_without_index_fails_before_the_slow_scan(monkeypatch):
    _no_index(monkeypatch)
    api = _api()

    with pytest.raises(EstimationIndexUnavailableError):
        sidecar_extent_provider(api, "csv")(UUID, KEY)

    api.get_temporal_extent.assert_not_called()


def test_zarr_key_uses_the_live_extent(monkeypatch):
    load = MagicMock()
    monkeypatch.setattr(estimation_index, "load_sidecar", load)
    api = _api()
    api.get_temporal_extent.return_value = (START, END)

    assert sidecar_extent_provider(api, "csv")(UUID, "test.zarr") == (START, END)
    load.assert_not_called()


def test_non_csv_parquet_key_uses_the_live_extent(monkeypatch):
    """A non-csv request is skipped later as "format not possible"; the extent
    lookup must not fail it first with "index missing"."""
    _no_index(monkeypatch)
    api = _api()
    api.get_temporal_extent.return_value = (START, END)

    assert sidecar_extent_provider(api, "geotiff")(UUID, KEY) == (START, END)


# --------------------------------------------------------------------------
# estimate_single_key_size - the parquet branch
# --------------------------------------------------------------------------


def _resolved() -> ResolvedSubsetRequest:
    return ResolvedSubsetRequest(
        uuid=UUID,
        keys=[KEY],
        start_date=pd.Timestamp("2000-01-01", tz="UTC"),
        end_date=pd.Timestamp("2030-01-01", tz="UTC"),
        bboxes=[],
        columns=None,
        geometry=None,
    )


def _api_with_parquet_key() -> MagicMock:
    api = _api()
    api.get_datasource.return_value = MagicMock(spec=ParquetDataSource)
    return api


def test_parquet_key_is_estimated_from_the_index(index):
    result = estimate_single_key_size(
        _api_with_parquet_key(), KEY, _resolved(), output_format="csv"
    )

    assert result["estimated_uncompressed_bytes"] == 35 * 100 + 50


def test_parquet_key_without_index_raises(monkeypatch):
    _no_index(monkeypatch)

    with pytest.raises(EstimationIndexUnavailableError):
        estimate_single_key_size(
            _api_with_parquet_key(), KEY, _resolved(), output_format="csv"
        )


def test_non_csv_format_raises_fast():
    """The frontend only requests csv for a parquet key; any other format is a
    malformed request and fails fast, before the date trim."""
    api = _api_with_parquet_key()

    with pytest.raises(ValueError, match=r"downloads from \.zarr keys only"):
        estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf")

    api.get_temporal_extent.assert_not_called()
