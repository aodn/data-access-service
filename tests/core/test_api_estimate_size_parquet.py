"""Unit tests for the estimate-size path (parquet).

Since #8927 there is exactly ONE parquet estimator: the pre-built index
(``core/estimation_index.py``). The old live scan - list the dataset on S3,
read the surviving files' footers, add up the row-group statistics - is gone,
so a key with no usable index no longer falls back to anything. It raises
``EstimationIndexUnavailableError`` and the request fails with that message.

The tests build a REAL index parquet in a tmp dir and query it with a real
DuckDB connection, because the things that go wrong here are arithmetic on the
cell grid (the two floors of ``_bin``) and the day-key filter - neither of
which a mocked query would exercise. Only the two things that would need AWS
are stubbed: the sidecar GET and the S3 secret.
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

from data_access_service import API
from data_access_service.core import estimation_index
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
)
from data_access_service.utils.cancellation import Cancellation, ClientGoneError
from data_access_service.utils.subset_request_resolver import ResolvedSubsetRequest

UUID = "test-uuid"
KEY = "test-key.parquet"

BIN_SIZE = 0.1

# The index rows every test starts from: two days, four cells, 100 rows each.
# lat/lon bins are cell indices, i.e. floor(degrees / BIN_SIZE).
#   cell (-360, 1500) is lat -36.0, lon 150.0
#   cell (-355, 1505) is lat -35.5, lon 150.5
INDEX_ROWS = [
    {"d": 20150101, "lat_bin": -360, "lon_bin": 1500, "c": 100},
    {"d": 20150101, "lat_bin": -355, "lon_bin": 1505, "c": 100},
    {"d": 20150102, "lat_bin": -360, "lon_bin": 1500, "c": 100},
    {"d": 20150102, "lat_bin": -355, "lon_bin": 1505, "c": 100},
]
TOTAL_ROWS = sum(row["c"] for row in INDEX_ROWS)

CSV_BYTES_PER_ROW = 120.5
CSV_HEADER_BYTES = 80
ZIP_RATIO = 0.12


# --------------------------------------------------------------------------
# Fixtures - a real index parquet, a real DuckDB, a stubbed sidecar
# --------------------------------------------------------------------------


class _StubClient:
    """Stands in for EstimationDuckDBClient: a plain in-memory DuckDB, and a
    no-op create_s3_secret so the test needs no AWS credentials."""

    def __init__(self):
        self._con = duckdb.connect(database=":memory:")
        self._con.execute("SET TimeZone = 'UTC';")
        self.secrets: list[str] = []

    def execute(self, sql: str, params=None):
        return (
            self._con.execute(sql) if params is None else self._con.execute(sql, params)
        )

    def create_s3_secret(self, bucket: str) -> None:
        self.secrets.append(bucket)

    def close(self) -> None:
        self._con.close()


def _sidecar(**overrides) -> EstimationSidecarMetadata:
    fields = {
        "version": ESTIMATION_INDEX_VERSION,
        "uuid": UUID,
        "key": KEY,
        "bin_size": BIN_SIZE,
        "bin_merge_factor": 1,
        "min_date": 20150101,
        "max_date": 20150102,
        "has_time": True,
        "total_rows": TOTAL_ROWS,
        "csv_bytes_per_row": CSV_BYTES_PER_ROW,
        "csv_header_bytes": CSV_HEADER_BYTES,
        "zip_ratio": ZIP_RATIO,
        "sample_rows": 100,
        "sample_files": 1,
        "null_position_rows": 0,
        "out_of_range_position_rows": 0,
        "null_time_rows": 0,
        "column_count": 5,
        "schema_fingerprint": "abc123",
        "last_updated": "2026-09-01T00:00:00Z",
    }
    fields.update(overrides)
    return EstimationSidecarMetadata(**fields)


@pytest.fixture
def index_file(tmp_path):
    """The index parquet itself, written to a local path."""

    def _write(rows=None) -> str:
        path = tmp_path / "index.parquet"
        pq.write_table(pa.Table.from_pylist(rows or INDEX_ROWS), path)
        return str(path)

    return _write


@pytest.fixture
def index(monkeypatch, index_file):
    """Wire up a usable index: local parquet, real DuckDB, stubbed sidecar.

    Returns a callable so a test can override the sidecar or the index rows.
    """
    client = _StubClient()
    estimation_index.set_duckdb_client(client)
    estimation_index.clear_sidecar_cache()

    def _setup(meta=None, rows=None) -> _StubClient:
        path = index_file(rows)
        monkeypatch.setattr(estimation_index, "index_s3_path", lambda u, k: path)
        monkeypatch.setattr(
            estimation_index, "load_sidecar", lambda u, k: meta or _sidecar()
        )
        return client

    yield _setup

    estimation_index.set_duckdb_client(None)
    estimation_index.clear_sidecar_cache()
    client.close()


def _api() -> API:
    """An API that cannot supply a live schema fingerprint, so the comparison in
    usable_sidecar is skipped (an empty fingerprint means "cannot compare")."""
    api = API()
    api.get_dataset_variables = MagicMock(return_value={})
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "TIME"))
    return api


def _estimate(
    api=None,
    bboxes=(),
    columns=None,
    output_format="csv",
    requested_end_date=None,
    **dates,
):
    return read_index_estimate(
        api or _api(),
        UUID,
        KEY,
        dates.get("date_start", pd.Timestamp("2015-01-01", tz="UTC")),
        dates.get("date_end", pd.Timestamp("2015-01-02 23:59:59", tz="UTC")),
        list(bboxes),
        output_format,
        columns=columns,
        requested_end_date=requested_end_date,
    )


# --------------------------------------------------------------------------
# The arithmetic: rows x measured width x measured zip ratio
# --------------------------------------------------------------------------


def test_estimate_is_rows_times_measured_width_plus_header(index):
    index()

    result = _estimate()

    assert result["uuid"] == UUID
    assert result["key"] == KEY
    assert result["format"] == "csv"
    expected_uncompressed = int(TOTAL_ROWS * CSV_BYTES_PER_ROW + CSV_HEADER_BYTES)
    assert result["estimated_uncompressed_bytes"] == expected_uncompressed
    assert result["estimated_output_bytes"] == int(expected_uncompressed * ZIP_RATIO)
    assert f"~{TOTAL_ROWS:,} rows" in result["notes"]
    assert "estimated from the pre-built index" in result["notes"]


def test_zero_rows_does_not_charge_the_header(index):
    """An empty result is 0 bytes, not a header's worth - the download writes
    nothing at all."""
    index()

    result = _estimate(
        date_start=pd.Timestamp("2019-01-01", tz="UTC"),
        date_end=pd.Timestamp("2019-12-31", tz="UTC"),
    )

    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0


def test_date_range_narrows_to_one_day(index):
    index()

    result = _estimate(
        date_start=pd.Timestamp("2015-01-01", tz="UTC"),
        date_end=pd.Timestamp("2015-01-01 23:59:59", tz="UTC"),
    )

    assert "~200 rows" in result["notes"]


def test_a_day_the_request_only_partly_covers_is_counted_whole(index):
    """Day granularity is an upper bound, and the notes must say so - otherwise
    a one-hour request looks exact."""
    index()

    result = _estimate(
        date_start=pd.Timestamp("2015-01-01 06:00:00", tz="UTC"),
        date_end=pd.Timestamp("2015-01-01 07:00:00", tz="UTC"),
    )

    assert "~200 rows" in result["notes"]
    assert "day granularity" in result["notes"]


# --------------------------------------------------------------------------
# Spatial pruning on the cell grid
# --------------------------------------------------------------------------


def test_bbox_keeps_only_the_cells_inside_it(index):
    index()
    # Covers cell (-360, 1500) only, not (-355, 1505).
    one_cell = BoundingBox(min_lon=150.0, min_lat=-36.0, max_lon=150.05, max_lat=-35.95)

    result = _estimate(bboxes=[one_cell])

    assert "~200 rows" in result["notes"]
    assert "bbox upper bound" in result["notes"]


def test_bbox_outside_the_data_estimates_zero(index):
    index()
    far_away = BoundingBox(min_lon=-50.0, min_lat=40.0, max_lon=-40.0, max_lat=50.0)

    result = _estimate(bboxes=[far_away])

    assert result["estimated_uncompressed_bytes"] == 0


def test_narrower_bbox_never_estimates_more_than_no_filter(index):
    index()
    narrow = BoundingBox(min_lon=150.0, min_lat=-36.0, max_lon=150.05, max_lat=-35.95)

    unfiltered = _estimate()
    filtered = _estimate(bboxes=[narrow])

    assert 0 < filtered["estimated_uncompressed_bytes"]
    assert (
        filtered["estimated_uncompressed_bytes"]
        <= unfiltered["estimated_uncompressed_bytes"]
    )


def test_empty_bboxes_means_no_spatial_filter_not_whole_globe(index):
    """[] is 'no spatial filter'; it must not become a whole-globe box, and it
    must not prune anything away."""
    index()

    no_filter = _estimate(bboxes=[])
    whole_globe = _estimate(
        bboxes=[BoundingBox(min_lon=-180.0, min_lat=-90.0, max_lon=180.0, max_lat=90.0)]
    )

    assert (
        no_filter["estimated_uncompressed_bytes"]
        == whole_globe["estimated_uncompressed_bytes"]
    )
    assert "bbox upper bound" not in no_filter["notes"]


def test_overlapping_bboxes_count_a_shared_cell_once(index):
    """The bboxes are one OR chain, so a cell both boxes cover must not be
    double-counted - which is what summing per-box estimates would do."""
    index()
    box = BoundingBox(min_lon=150.0, min_lat=-36.0, max_lon=150.6, max_lat=-35.4)
    overlapping = BoundingBox(
        min_lon=150.3, min_lat=-35.8, max_lon=150.9, max_lat=-35.0
    )

    one_box = _estimate(bboxes=[box])
    two_boxes = _estimate(bboxes=[box, overlapping])

    assert (
        two_boxes["estimated_uncompressed_bytes"]
        == one_box["estimated_uncompressed_bytes"]
    )
    assert "union of 2 polygon bboxes" in two_boxes["notes"]


def test_a_merged_cell_index_is_binned_at_the_merged_resolution(index):
    """bin_merge_factor coarsens the grid, so a request must bin at the merged
    resolution to hit the cells the file actually holds. Ignoring the factor
    bins -36.0 as base cell -360 instead of merged cell -36, which matches
    nothing and silently estimates zero.

    The index here holds merged cell -36 (base cells -360..-351, i.e. latitudes
    -36.0..-35.1), so a box on the -36.0 boundary must find it.
    """
    meta = _sidecar(bin_merge_factor=10)
    rows = [{"d": 20150101, "lat_bin": -36, "lon_bin": 150, "c": 100}]
    index(meta=meta, rows=rows)
    one_day = {
        "date_start": pd.Timestamp("2015-01-01", tz="UTC"),
        "date_end": pd.Timestamp("2015-01-01 23:59:59", tz="UTC"),
    }

    on_the_boundary = BoundingBox(
        min_lon=150.0, min_lat=-36.0, max_lon=150.05, max_lat=-35.95
    )
    result = _estimate(bboxes=[on_the_boundary], **one_day)

    assert "~100 rows" in result["notes"]
    # The note must quote the MERGED width, not the build's base bin_size.
    assert "cells of 1 deg" in result["notes"]

    # The cell below it (latitudes -37.0..-36.1) is a different merged cell.
    neighbour = BoundingBox(min_lon=150.0, min_lat=-36.9, max_lon=150.05, max_lat=-36.5)
    assert _estimate(bboxes=[neighbour], **one_day)["estimated_output_bytes"] == 0


# --------------------------------------------------------------------------
# Timeless datasets and the extrapolated tail
# --------------------------------------------------------------------------


def test_timeless_dataset_ignores_the_date_filter(index):
    """has_time=False means the index holds one synthetic day key, so filtering
    on dates would drop everything. The download does not filter it either."""
    index(meta=_sidecar(has_time=False))

    result = _estimate(
        date_start=pd.Timestamp("2019-01-01", tz="UTC"),
        date_end=pd.Timestamp("2019-12-31", tz="UTC"),
    )

    assert f"~{TOTAL_ROWS:,} rows" in result["notes"]
    assert "day granularity" not in result["notes"]


def test_days_after_the_index_are_extrapolated_not_dropped(index):
    """The index is a weekly snapshot. A request running past its last covered
    day must be charged for those days, because an estimate that is too small
    means the user gets a much bigger download than promised."""
    index()

    covered_only = _estimate()
    past_the_index = _estimate(
        date_end=pd.Timestamp("2015-01-04 23:59:59", tz="UTC"),
        requested_end_date=pd.Timestamp("2015-01-04 23:59:59", tz="UTC"),
    )

    assert (
        past_the_index["estimated_uncompressed_bytes"]
        > covered_only["estimated_uncompressed_bytes"]
    )
    assert "2 later day(s) extrapolated" in past_the_index["notes"]


def test_no_extrapolation_when_the_request_ends_inside_the_index(index):
    index()

    result = _estimate(requested_end_date=pd.Timestamp("2015-01-02 23:59:59", tz="UTC"))

    assert "extrapolated" not in result["notes"]


def test_timeless_dataset_is_never_extrapolated(index):
    """A synthetic day key says nothing about the real extent, so there is no
    daily rate to extrapolate from."""
    index(meta=_sidecar(has_time=False))

    result = _estimate(requested_end_date=pd.Timestamp("2030-01-01", tz="UTC"))

    assert "extrapolated" not in result["notes"]


# --------------------------------------------------------------------------
# No fallback: an unusable index fails the request (#8927)
# --------------------------------------------------------------------------


def test_missing_sidecar_raises(index, monkeypatch):
    index()
    monkeypatch.setattr(estimation_index, "load_sidecar", lambda u, k: None)

    with pytest.raises(EstimationIndexUnavailableError, match="no usable pre-built"):
        _estimate()


def test_unknown_index_version_raises(index):
    index(meta=_sidecar(version=ESTIMATION_INDEX_VERSION + 1))

    with pytest.raises(EstimationIndexUnavailableError):
        _estimate()


def test_changed_column_set_raises(index):
    """A different schema makes the measured csv_bytes_per_row stale, so the
    index must be rejected rather than used at the wrong width."""
    index(meta=_sidecar(schema_fingerprint="built-for-other-columns"))
    api = _api()
    api.get_dataset_variables = MagicMock(
        return_value={UUID: {KEY: ["TIME", "LATITUDE", "LONGITUDE"]}}
    )

    with pytest.raises(EstimationIndexUnavailableError):
        _estimate(api=api)


def test_non_csv_format_is_rejected(index):
    """The index only models the zipped-CSV download."""
    index()

    with pytest.raises(EstimationIndexUnavailableError):
        _estimate(output_format="netcdf")


def test_query_failure_raises(index, monkeypatch):
    index()
    failing = MagicMock()
    failing.execute.side_effect = RuntimeError("s3 is unhappy")
    monkeypatch.setattr(estimation_index, "_get_client", lambda: failing)
    monkeypatch.setattr(estimation_index, "_ensure_secret", lambda c: None)

    with pytest.raises(EstimationIndexUnavailableError, match="could not query"):
        _estimate()


def test_broken_read_path_raises_and_logs_at_error(index, monkeypatch, caplog):
    """A TypeError here means this module is broken, not that the index is
    missing. It must be logged at ERROR so a rename cannot pass for a dataset
    that was simply never indexed."""
    index()
    broken = MagicMock()
    broken.execute.side_effect = TypeError("signature changed")
    monkeypatch.setattr(estimation_index, "_get_client", lambda: broken)
    monkeypatch.setattr(estimation_index, "_ensure_secret", lambda c: None)

    with caplog.at_level("ERROR"):
        with pytest.raises(
            EstimationIndexUnavailableError, match="read path is broken"
        ):
            _estimate()

    assert "is broken" in caplog.text


def test_unavailable_index_fails_the_whole_request(index, monkeypatch):
    """estimate_datasets_size skips a ValueError key as "cannot produce this
    format" and carries on with the rest. A missing index must NOT be swallowed
    that way: the request has to fail so the frontend can say why. Checked
    through estimate_datasets_size, because that except clause is the thing
    that could quietly absorb it."""
    index()
    monkeypatch.setattr(estimation_index, "load_sidecar", lambda u, k: None)
    api = _single_key_api(index)
    monkeypatch.setattr(
        "data_access_service.core.api.resolve_subset_request",
        lambda **kwargs: _resolved(),
    )

    with pytest.raises(EstimationIndexUnavailableError):
        api.estimate_datasets_size(
            UUID, keys=[KEY], output_format="csv", multi_polygon=None
        )


# --------------------------------------------------------------------------
# estimate_single_key_size - the parquet branch as a whole
# --------------------------------------------------------------------------


def _parquet_datasource() -> MagicMock:
    """spec=ParquetDataSource so isinstance() picks the parquet branch."""
    return MagicMock(spec=ParquetDataSource)


def _resolved(**overrides) -> ResolvedSubsetRequest:
    fields = {
        "uuid": UUID,
        "keys": [KEY],
        "start_date": pd.Timestamp("2015-01-01", tz="UTC"),
        "end_date": pd.Timestamp("2015-01-02 23:59:59", tz="UTC"),
        "bboxes": [],
        "columns": None,
        "geometry": None,
    }
    fields.update(overrides)
    return ResolvedSubsetRequest(**fields)


def _single_key_api(index_setup) -> API:
    api = _api()
    api.get_datasource = MagicMock(return_value=_parquet_datasource())
    api.get_temporal_extent = MagicMock(
        return_value=(
            pd.Timestamp("2015-01-01", tz="UTC"),
            pd.Timestamp("2015-01-02 23:59:59", tz="UTC"),
        )
    )
    return api


def test_parquet_branch_goes_through_the_index(index):
    index()
    api = _single_key_api(index)

    result = estimate_single_key_size(api, KEY, _resolved(), output_format="csv")

    assert "estimated from the pre-built index" in result["notes"]
    assert result["estimated_output_bytes"] > 0


def test_columns_ignored_and_noted(index):
    """The download's query_data passes no columns, so the CSV carries every
    column; the estimate must stay aligned with that rather than shrink."""
    index()
    api = _single_key_api(index)

    with_columns = estimate_single_key_size(
        api, KEY, _resolved(columns=["TEMP"]), output_format="csv"
    )
    without = estimate_single_key_size(api, KEY, _resolved(), output_format="csv")

    assert (
        with_columns["estimated_uncompressed_bytes"]
        == without["estimated_uncompressed_bytes"]
    )
    assert "column subsetting not supported yet" in with_columns["notes"]


def test_non_csv_format_raises_fast_before_any_index_work(index):
    """The frontend only requests csv for a parquet key; any other format is a
    malformed request and must fail before the date trim. This stays a
    ValueError (a format problem, not a missing index), so the other keys of a
    multi-key request still get estimated."""
    index()
    api = _single_key_api(index)

    with pytest.raises(ValueError, match=r"downloads from \.zarr keys only"):
        estimate_single_key_size(api, KEY, _resolved(), output_format="netcdf")

    api.get_temporal_extent.assert_not_called()


def test_request_outside_the_temporal_extent_is_a_zero_estimate(index):
    """has_data False: resolve_subset_request already found the range empty, so
    there is nothing to ask the index."""
    index()
    api = _single_key_api(index)

    result = estimate_single_key_size(
        api,
        KEY,
        _resolved(start_date=None, end_date=None),
        output_format="csv",
    )

    assert result["estimated_output_bytes"] == 0


def test_missing_key_returns_none(index):
    index()
    api = _single_key_api(index)
    api.get_datasource = MagicMock(return_value=None)

    assert estimate_single_key_size(api, KEY, _resolved(), output_format="csv") is None


def test_cancelled_client_stops_before_the_index_query(index):
    """The index query is one DuckDB call, so there is no inner loop to stop in
    any more - but a client that has already gone must not start the work."""
    index()
    api = _single_key_api(index)
    cancellation = Cancellation()
    cancellation.cancel()

    with pytest.raises(ClientGoneError):
        estimate_single_key_size(
            api, KEY, _resolved(), output_format="csv", cancellation=cancellation
        )


# --------------------------------------------------------------------------
# sidecar_extent_provider - the estimate's temporal extent
# --------------------------------------------------------------------------


def test_extent_comes_from_the_sidecar_without_scanning(index):
    """The sidecar's day range saves the slow live extent scan - the one the
    estimate used to run twice per request."""
    index()
    api = _api()
    api.get_temporal_extent = MagicMock()

    start, end = sidecar_extent_provider(api)(UUID, KEY)

    api.get_temporal_extent.assert_not_called()
    assert start == pd.Timestamp("2015-01-01", tz="UTC")
    assert end.date() == pd.Timestamp("2015-01-02").date()


def test_extent_still_falls_back_to_the_live_scan(index, monkeypatch):
    """Unlike the estimate, the EXTENT keeps its fallback: it is a real
    property of the data, and api.get_temporal_extent can always answer it."""
    index()
    monkeypatch.setattr(estimation_index, "load_sidecar", lambda u, k: None)
    live = (
        pd.Timestamp("2014-01-01", tz="UTC"),
        pd.Timestamp("2014-12-31", tz="UTC"),
    )
    api = _api()
    api.get_temporal_extent = MagicMock(return_value=live)

    assert sidecar_extent_provider(api)(UUID, KEY) == live


def test_timeless_dataset_uses_the_live_extent(index):
    """A synthetic day key says nothing about the data's real extent."""
    index(meta=_sidecar(has_time=False))
    live = (
        pd.Timestamp("2014-01-01", tz="UTC"),
        pd.Timestamp("2014-12-31", tz="UTC"),
    )
    api = _api()
    api.get_temporal_extent = MagicMock(return_value=live)

    assert sidecar_extent_provider(api)(UUID, KEY) == live
