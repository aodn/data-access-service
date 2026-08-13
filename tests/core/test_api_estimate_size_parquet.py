"""Unit tests for the estimate-size path (parquet).

The parquet branch never reads a data page: it prunes to the requested
partitions, reads the surviving files' FOOTERS for a row count, and multiplies
that by a per-row CSV width derived from the schema.

Most tests run against the real parquet trees under ``tests/canned`` rather
than mocks, because the things that can go wrong here - hive partition columns
being materialised into the output, statistics being absent, a partition value
typed as string in one dataset and int32 in another - only exist in real files.
"""

import io

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pytest
from unittest.mock import MagicMock

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

from data_access_service import API
from data_access_service.core.constants import (
    ASSUMED_STRING_BYTES,
    COMPRESSION_RATIO_CSV_GZIP,
    CSV_BYTES_PER_BOOL,
    CSV_BYTES_PER_DATE,
    CSV_BYTES_PER_FLOAT32,
    CSV_BYTES_PER_FLOAT64,
    CSV_BYTES_PER_INT,
    CSV_BYTES_PER_NULL,
    CSV_BYTES_PER_TIMESTAMP,
    CSV_SEPARATOR_BYTES,
)
from data_access_service.core.size_estimation import (
    _as_utc_timestamp,
    _csv_bytes_per_row,
    _csv_value_bytes,
    _estimate_parquet_size,
    _partition_value_widths,
    estimate_single_key_size,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.cancellation import Cancellation, ClientGoneError
from data_access_service.utils.subset_request_resolver import ResolvedSubsetRequest

CANNED = {
    "argo": "tests/canned/s3_sample_edge_cases/argo.parquet",
    "mooring": "tests/canned/s3_sample2/mooring_temperature_logger_delayed_qc.parquet",
    "seabird": "tests/canned/s3_sample1/aggregated_seabird_nonqc.parquet",
    "auv": "tests/canned/s3_sample1/autonomous_underwater_vehicle.parquet",
    "acoustic": "tests/canned/s3_sample1/animal_acoustic_tracking_delayed_qc.parquet",
}

UUID = "test-uuid"
KEY = "test-key"


def _canned(name: str) -> pa_ds.Dataset:
    return pa_ds.dataset(CANNED[name], format="parquet", partitioning="hive")


def _real_csv_bytes_per_row(dataset: pa_ds.Dataset) -> float:
    """Actual CSV bytes per row, by materialising the dataset the way the
    download does (to_table -> pandas -> to_csv). Test-only: the estimator must
    never do this."""
    df = dataset.to_table().to_pandas()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return len(buffer.getvalue().encode()) / len(df)


def _api() -> API:
    """An API whose dim-name lookup answers with the standard names; the canned
    datasets use TIME/LATITUDE/LONGITUDE (argo uses JULD, handled per test)."""
    api = API()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "TIME"))
    return api


def _parquet_datasource(dataset: pa_ds.Dataset) -> MagicMock:
    """spec=ParquetDataSource so isinstance() passes; only `.dataset` is read."""
    source = MagicMock(spec=ParquetDataSource)
    source.dataset = dataset
    return source


def _estimate(
    dataset,
    api=None,
    bboxes=(),
    columns=None,
    output_format="csv",
    cancellation=None,
    **dates,
):
    return _estimate_parquet_size(
        api or _api(),
        _parquet_datasource(dataset),
        UUID,
        KEY,
        dates.get("date_start", pd.Timestamp("2000-01-01", tz="UTC")),
        dates.get("date_end", pd.Timestamp("2030-01-01", tz="UTC")),
        list(bboxes),
        columns,
        output_format,
        cancellation,
    )


# --------------------------------------------------------------------------
# _csv_value_bytes / _csv_bytes_per_row - the row width
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pa_type, expected",
    [
        (pa.float64(), CSV_BYTES_PER_FLOAT64),
        (pa.float32(), CSV_BYTES_PER_FLOAT32),
        (pa.int8(), CSV_BYTES_PER_INT[8]),
        (pa.int16(), CSV_BYTES_PER_INT[16]),
        (pa.int32(), CSV_BYTES_PER_INT[32]),
        (pa.int64(), CSV_BYTES_PER_INT[64]),
        (pa.uint8(), CSV_BYTES_PER_INT[8]),
        (pa.bool_(), CSV_BYTES_PER_BOOL),
        (pa.timestamp("ns"), CSV_BYTES_PER_TIMESTAMP),
        (pa.date32(), CSV_BYTES_PER_DATE),
        (pa.null(), CSV_BYTES_PER_NULL),
        (pa.string(), ASSUMED_STRING_BYTES),
        (pa.large_string(), ASSUMED_STRING_BYTES),
        # Unmodelled types fall back rather than raising.
        (pa.list_(pa.int32()), ASSUMED_STRING_BYTES),
    ],
)
def test_csv_value_bytes_per_type(pa_type, expected):
    assert _csv_value_bytes(pa_type) == expected


def test_csv_value_bytes_are_wide_enough_for_the_widest_value():
    """The widths are upper bounds, so the printed form of an extreme value of
    each type must still fit. This is what lets the estimate claim upper bound."""
    widest = {
        pa.float64(): -1.2345678901234567e-308,
        pa.float32(): -3.4028235e38,
        pa.int8(): -128,
        pa.int16(): -32768,
        pa.int32(): -(2**31),
        pa.int64(): -(2**63),
        pa.bool_(): False,
        pa.timestamp("ns"): pd.Timestamp("2020-01-01 00:00:00.000000001"),
        pa.date32(): pd.Timestamp("2020-12-31").date(),
    }
    for pa_type, value in widest.items():
        assert len(str(value)) <= _csv_value_bytes(pa_type), pa_type


def test_csv_bytes_per_row_charges_every_column_plus_a_separator():
    schema = pa.schema(
        [
            pa.field("TEMP", pa.float64()),
            pa.field("QC", pa.int8()),
            pa.field("TIME", pa.timestamp("ns")),
        ]
    )
    dataset = pa_ds.dataset([], schema=schema, format="parquet")

    expected = (
        CSV_BYTES_PER_FLOAT64
        + CSV_BYTES_PER_INT[8]
        + CSV_BYTES_PER_TIMESTAMP
        + 3 * CSV_SEPARATOR_BYTES
    )
    assert _csv_bytes_per_row(dataset) == expected


def test_partition_columns_are_measured_not_assumed():
    """The defect this guards: `polygon` is a ~200-character WKB hex string on
    EVERY row of the CSV, but it has no storage inside the parquet files (its
    value is the directory name), so neither the footers nor ASSUMED_STRING_BYTES
    account for it. Charging it the string default under-counted every row."""
    dataset = _canned("mooring")

    widths = _partition_value_widths(dataset)

    assert set(widths) == {"site_code", "timestamp", "polygon"}
    # The real WKB hex, far wider than the string default.
    assert widths["polygon"] > 150
    assert widths["polygon"] > ASSUMED_STRING_BYTES
    # And the row width actually uses it.
    assert _csv_bytes_per_row(dataset) > widths["polygon"]


def test_partition_value_widths_empty_when_not_partitioned():
    schema = pa.schema([pa.field("TEMP", pa.float64())])
    assert _partition_value_widths(pa_ds.dataset([], schema=schema)) == {}


@pytest.mark.parametrize("name", sorted(CANNED))
def test_row_width_never_under_estimates_real_csv(name):
    """Calibration guard against real files: the estimate may be generous, but
    it must never promise fewer bytes than the download actually writes.

    The previous implementation scaled the row groups' uncompressed bytes by a
    flat 2.0, which came out 4-15x BELOW the real CSV because dictionary and RLE
    encoding make binary column chunks far smaller than the text they print as.
    """
    dataset = _canned(name)

    estimated = _csv_bytes_per_row(dataset)
    real = _real_csv_bytes_per_row(dataset)

    assert estimated >= real, f"{name}: under-estimated {estimated} < {real}"
    # Guard the other side too, so the widths cannot drift into uselessness.
    assert estimated <= real * 5, f"{name}: over-estimated {estimated / real:.1f}x"


# --------------------------------------------------------------------------
# _estimate_parquet_size - the whole parquet branch
# --------------------------------------------------------------------------


def test_estimate_matches_rows_times_width_and_zip_ratio():
    dataset = _canned("mooring")
    total_rows = dataset.count_rows()

    result = _estimate(dataset)

    assert result["uuid"] == UUID
    assert result["key"] == KEY
    assert result["format"] == "csv"
    # Whole-dataset request: every row survives, so the arithmetic is exact.
    assert result["estimated_uncompressed_bytes"] == total_rows * _csv_bytes_per_row(
        dataset
    )
    assert result["estimated_output_bytes"] == int(
        result["estimated_uncompressed_bytes"] * COMPRESSION_RATIO_CSV_GZIP
    )
    assert f"~{total_rows:,} rows" in result["notes"]


def test_estimate_is_at_least_the_real_csv_size():
    """End to end on a real tree: uncompressed bytes must cover the CSV the
    download would write."""
    dataset = _canned("mooring")
    df = dataset.to_table().to_pandas()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    result = _estimate(dataset)

    assert result["estimated_uncompressed_bytes"] >= len(buffer.getvalue().encode())


def test_date_range_outside_data_prunes_to_zero():
    """mooring covers 2014-11..2015-03; a 2019 request must survive no row
    group. Zero rows is a zero estimate, not an error."""
    dataset = _canned("mooring")

    result = _estimate(
        dataset,
        date_start=pd.Timestamp("2019-01-01", tz="UTC"),
        date_end=pd.Timestamp("2019-12-31", tz="UTC"),
    )

    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_output_bytes"] == 0


def test_bbox_not_intersecting_any_partition_returns_empty_estimate():
    """No polygon partition intersects -> PolygonNotIntersectingError inside,
    surfaced as a zero estimate rather than a 500."""
    dataset = _canned("mooring")  # sits near 150E/36S
    far_away = BoundingBox(min_lon=-50.0, min_lat=40.0, max_lon=-40.0, max_lat=50.0)

    result = _estimate(dataset, bboxes=[far_away])

    assert result["estimated_uncompressed_bytes"] == 0
    assert "no data partitions intersect" in result["notes"]


def test_bbox_covering_the_data_keeps_it_and_notes_the_upper_bound():
    dataset = _canned("mooring")
    covering = BoundingBox(min_lon=149.0, min_lat=-37.0, max_lon=151.0, max_lat=-35.0)

    result = _estimate(dataset, bboxes=[covering])

    assert result["estimated_uncompressed_bytes"] > 0
    assert "bbox upper bound" in result["notes"]


def test_narrower_bbox_never_estimates_more_than_no_filter():
    dataset = _canned("argo")
    api = _api()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "JULD"))
    narrow = BoundingBox(min_lon=100.0, min_lat=-20.0, max_lon=110.0, max_lat=-10.0)

    unfiltered = _estimate(dataset, api=api)
    filtered = _estimate(dataset, api=api, bboxes=[narrow])

    assert 0 < filtered["estimated_uncompressed_bytes"]
    assert (
        filtered["estimated_uncompressed_bytes"]
        <= unfiltered["estimated_uncompressed_bytes"]
    )


def test_empty_bboxes_means_no_spatial_filter_not_whole_globe():
    """[] is 'no spatial filter' - it must not be turned into a whole-globe box,
    and it must not prune anything away."""
    dataset = _canned("mooring")

    no_filter = _estimate(dataset, bboxes=[])
    whole_globe = _estimate(
        dataset,
        bboxes=[
            BoundingBox(min_lon=-180.0, min_lat=-90.0, max_lon=180.0, max_lat=90.0)
        ],
    )

    assert (
        no_filter["estimated_uncompressed_bytes"]
        == whole_globe["estimated_uncompressed_bytes"]
    )
    assert "bbox upper bound" not in no_filter["notes"]


def test_columns_ignored_and_noted():
    """The download's query_data passes no columns, so the CSV carries every
    column; the estimate must stay aligned with that rather than shrink."""
    dataset = _canned("mooring")

    with_columns = _estimate(dataset, columns=["TEMP"])
    without = _estimate(dataset)

    assert (
        with_columns["estimated_uncompressed_bytes"]
        == without["estimated_uncompressed_bytes"]
    )
    assert "column subsetting not supported yet" in with_columns["notes"]


def test_non_csv_format_raises_fast():
    """The frontend only requests csv for a parquet key; any other format is a
    malformed request and fails fast, before the date trim or any pruning."""
    api = _api()
    api.get_datasource = MagicMock(return_value=_parquet_datasource(_canned("mooring")))
    api.get_temporal_extent = MagicMock()
    resolved = ResolvedSubsetRequest(
        uuid=UUID,
        keys=[KEY],
        start_date=pd.Timestamp("2000-01-01", tz="UTC"),
        end_date=pd.Timestamp("2030-01-01", tz="UTC"),
        bboxes=[],
        columns=None,
        geometry=None,
    )

    with pytest.raises(ValueError, match=r"downloads from \.zarr keys only"):
        estimate_single_key_size(api, KEY, resolved, output_format="netcdf")

    api.get_temporal_extent.assert_not_called()


def test_multiple_bboxes_noted_as_a_union():
    dataset = _canned("mooring")
    boxes = [
        BoundingBox(min_lon=149.0, min_lat=-37.0, max_lon=151.0, max_lat=-35.0),
        BoundingBox(min_lon=150.0, min_lat=-36.5, max_lon=152.0, max_lat=-34.0),
    ]

    result = _estimate(dataset, bboxes=boxes)

    assert "union of 2 polygon bboxes" in result["notes"]


def test_string_typed_timestamp_partition_does_not_raise():
    """Hive partition columns are typed from the directory names, so `timestamp`
    is int32 in one dataset and a string in another. Comparing an int64 literal
    against a string field raises ArrowNotImplementedError - _timestamp_partition_scalar
    casts to the dataset's own type instead."""
    dataset = pa_ds.dataset(
        CANNED["mooring"],
        format="parquet",
        partitioning=pa_ds.partitioning(
            pa.schema(
                [
                    pa.field("site_code", pa.string()),
                    pa.field("timestamp", pa.string()),
                    pa.field("polygon", pa.string()),
                ]
            ),
            flavor="hive",
        ),
    )

    result = _estimate(dataset)

    assert result["estimated_uncompressed_bytes"] > 0


def test_sampling_kicks_in_above_the_footer_read_cap(monkeypatch):
    """argo has 105 fragments; drop the cap below that and the estimate must
    switch to sampling, say so, and still land near the unsampled figure."""
    import data_access_service.core.size_estimation as size_estimation

    api = _api()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "JULD"))
    dataset = _canned("argo")

    full = _estimate(dataset, api=api)

    monkeypatch.setattr(size_estimation, "MAX_FRAGMENT_FOOTER_READS", 10)
    sampled = _estimate(dataset, api=api)

    assert "sampled 10 of 105 files, extrapolated" in sampled["notes"]
    assert sampled["estimated_uncompressed_bytes"] == pytest.approx(
        full["estimated_uncompressed_bytes"], rel=0.5
    )


# --------------------------------------------------------------------------
# _as_utc_timestamp - reading time statistics out of a footer
# --------------------------------------------------------------------------


def test_naive_statistic_is_assumed_utc():
    """pyarrow hands back a NAIVE Timestamp for a timestamp column, which cannot
    be compared against the request's UTC bounds."""
    result = _as_utc_timestamp(pd.Timestamp("2020-06-01 12:00:00"))

    assert result == pd.Timestamp("2020-06-01 12:00:00", tz="UTC")


def test_aware_statistic_is_comparable_with_utc_bounds():
    aware = pd.Timestamp("2020-06-01 12:00:00", tz="Australia/Hobart")

    result = _as_utc_timestamp(aware)

    assert result == aware


@pytest.mark.parametrize("value", [1234567890, 1.5, "2020-01-01", None, b"x"])
def test_non_date_statistic_returns_none(value):
    """A time column stored as a plain number (epoch seconds) is ambiguous - we
    refuse to guess a unit, so the caller keeps the row group."""
    assert _as_utc_timestamp(value) is None


# --------------------------------------------------------------------------
# Cancellation - the fragment loop is where a cancelled estimate spends its time
# --------------------------------------------------------------------------


class _CancelAfter(Cancellation):
    """A client that disconnects after `checkpoints` checks have gone by."""

    def __init__(self, checkpoints: int):
        super().__init__()
        self.checks = 0
        self._checkpoints = checkpoints

    def raise_if_client_gone(self) -> None:
        self.checks += 1
        if self.checks > self._checkpoints:
            self.cancel()
        super().raise_if_client_gone()


def test_cancelled_client_stops_the_fragment_loop():
    """argo has 105 fragments, so a footer read each. Once the client goes, the
    estimate must stop at the next fragment instead of reading the rest."""
    api = _api()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "JULD"))
    cancellation = _CancelAfter(3)

    with pytest.raises(ClientGoneError):
        _estimate(_canned("argo"), api=api, cancellation=cancellation)

    # Stopped on the 4th check, not after all 105 fragments.
    assert cancellation.checks == 4


def test_fragment_loop_checks_every_fragment():
    api = _api()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "JULD"))
    cancellation = _CancelAfter(10_000)  # never actually cancels

    result = _estimate(_canned("argo"), api=api, cancellation=cancellation)

    assert cancellation.checks == 105
    assert result["estimated_uncompressed_bytes"] > 0


def test_estimate_without_cancellation_is_unaffected():
    api = _api()
    api.resolve_dim_names = MagicMock(return_value=("LATITUDE", "LONGITUDE", "JULD"))

    with_none = _estimate(_canned("argo"), api=api, cancellation=None)
    live = _estimate(_canned("argo"), api=api, cancellation=Cancellation())

    assert with_none == live
