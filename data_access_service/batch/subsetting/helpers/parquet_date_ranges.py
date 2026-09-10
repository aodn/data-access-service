"""Date-range preparation for the PARQUET batch download."""

import heapq
import logging
import numpy as np
import pandas as pd
import pytz

from typing import Tuple
import pyarrow.dataset as ds
from pandas._libs import NaTType
from pyarrow import compute as pc
from tenacity import retry, stop_after_attempt, wait_exponential

from aodn_cloud_optimised.lib.DataQuery import (
    DateOutOfRangeError,
    get_temporal_extent,
    get_timestamps_boundary_values,
    create_time_filter,
    ParquetDataSource,
)

from data_access_service.batch.subsetting.helpers.time_column import (
    TimeColumn,
    resolve_time_column,
)
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    MAX_PARQUET_SPLIT,
    PARQUET_INDEX_SUBSET_ROW_NUMBER,
    PARQUET_SUBSET_ROW_NUMBER,
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
)
from data_access_service.core.estimation_index import (
    count_index_rows,
    index_coverage_end,
    sidecar_for_row_counts,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.estimation_types import EstimationSidecarMetadata
from data_access_service.utils.date_time_utils import (
    ensure_timezone,
    split_date_range_binary,
    to_naive_utc_string,
)
from data_access_service.utils.multi_polygon_helper import bbox_of

log = logging.getLogger(__name__)


# Bug: count_rows() failures used to be caught and silently skipped, dropping that
# month's data with no error -- just a smaller row count later. This bug always
# existed but rarely triggered; the tiler suite added more test load, making
# transient S3 errors in CI more likely and finally exposing it.
# Fix: retry transient failures, and raise if still failing after retries instead
# of silently dropping data.
COUNT_ROWS_MAX_ATTEMPTS = 3
COUNT_ROWS_MIN_WAIT_SECONDS = 2
COUNT_ROWS_MAX_WAIT_SECONDS = 10


def _log_count_rows_retry(retry_state):
    log.warning(
        f"[Retry] dataset.count_rows() failed on attempt "
        f"#{retry_state.attempt_number}: {retry_state.outcome.exception()}. Retrying..."
    )


@retry(
    stop=stop_after_attempt(COUNT_ROWS_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=1, min=COUNT_ROWS_MIN_WAIT_SECONDS, max=COUNT_ROWS_MAX_WAIT_SECONDS
    ),
    before_sleep=_log_count_rows_retry,
    reraise=True,
)
def _count_rows_with_retry(dataset, time_filter) -> int:
    return dataset.count_rows(filter=time_filter)


def _spatial_bbox_filter(
    lat_dim: str, lon_dim: str, bbox: BoundingBox
) -> ds.Expression:
    return (
        (pc.field(lat_dim) >= bbox.min_lat)
        & (pc.field(lat_dim) <= bbox.max_lat)
        & (pc.field(lon_dim) >= bbox.min_lon)
        & (pc.field(lon_dim) <= bbox.max_lon)
    )


def _same_utc_day(start: pd.Timestamp, end: pd.Timestamp) -> bool:
    """True when both ends fall on the same UTC calendar day.

    The estimation index is day-granular: splitting inside a day does not
    change SUM(c), so the download falls back to a live count instead.
    """
    return start.floor("D") == end.floor("D")


def _split_on_utc_day_boundary(
    start: pd.Timestamp, end: pd.Timestamp
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    """Split a multi-day range at a UTC midnight, not at the timestamp midpoint.

    The estimation index keys rows by calendar day, so a binary time split
    around midnight still covers the same day keys and SUM(c) never falls.
    Cutting at the midpoint midnight separates the days in one step.
    """
    start = ensure_timezone(pd.Timestamp(start))
    end = ensure_timezone(pd.Timestamp(end))
    start_day = start.floor("D")
    end_day = end.floor("D")
    if start_day == end_day:
        raise ValueError(
            f"Range is a single UTC day; cannot split on a day boundary: "
            f"{start} to {end}"
        )
    days = (end_day - start_day).days
    mid = start_day + pd.Timedelta(days=(days + 1) // 2)
    left_end = mid - pd.Timedelta(nanoseconds=1)
    if left_end < start or mid > end:
        raise ValueError(
            f"Range too short to split on a UTC day boundary: {start} to {end}"
        )
    return start, left_end, mid, end


def _enqueue_split(
    q: list,
    start: pd.Timestamp,
    end: pd.Timestamp,
    times_of_split: int,
    num_rows: int,
    row_limit: int,
    force_live: bool,
    checked_date_ranges: list[dict],
    split_fn=None,
) -> None:
    """Replace a too-large range with two non-overlapping halves, or keep it."""
    if split_fn is None:
        split_fn = split_date_range_binary
    try:
        left_start, left_end, right_start, right_end = split_fn(start, end)
        log.info(
            "Range too large (%s rows > limit %s); discarding parent "
            "[%s → %s] and enqueueing non-overlapping halves "
            "[%s → %s] and [%s → %s] (split depth %s → %s)",
            num_rows,
            row_limit,
            start,
            end,
            left_start,
            left_end,
            right_start,
            right_end,
            times_of_split,
            times_of_split + 1,
        )
        heapq.heappush(q, (left_start, left_end, times_of_split + 1, force_live))
        heapq.heappush(q, (right_start, right_end, times_of_split + 1, force_live))
    except Exception as e:
        log.warning(f"Could not split range {start} to {end}: {e}")
        checked_date_ranges.append({"start_date": start, "end_date": end})


def _live_time_filter(
    dataset,
    start,
    end,
    time_dim: str,
    time_column: TimeColumn,
) -> ds.Expression | None:
    """Time filter for a live count, or None when the range has no overlap.

    String time columns cannot go through create_time_filter (no pyarrow
    kernel for string vs timestamp; issue 9144).
    """
    start_str = to_naive_utc_string(start)
    end_str = to_naive_utc_string(end)

    if time_column.is_string:
        try:
            return create_customised_time_filter(
                dataset=dataset,
                start=start,
                end=end,
                time_varname=time_dim,
                time_column=time_column,
            )
        except ValueError as e:
            log.info(
                "Skipping date range %s to %s: no overlap with dataset extent (%s)",
                start,
                end,
                e,
            )
            return None

    try:
        return create_time_filter(
            dataset=dataset,
            date_start=start_str,
            date_end=end_str,
            time_varname=time_dim,
        )
    except DateOutOfRangeError as e:
        # create_time_filter validates against partition/temporal bounds and can
        # raise false positives; fall back to a filter clamped to real extent.
        log.info(
            "create_time_filter out of range for %s to %s (%s); "
            "trying customised time filter",
            start_str,
            end_str,
            e,
        )
        try:
            return create_customised_time_filter(
                dataset=dataset,
                start=start,
                end=end,
                time_varname=time_dim,
                time_column=time_column,
            )
        except ValueError as e2:
            log.info(
                "Skipping date range %s to %s: no overlap with dataset extent (%s)",
                start,
                end,
                e2,
            )
            return None


def _live_count_rows(
    dataset,
    start,
    end,
    time_dim: str,
    time_column: TimeColumn,
    lat_dim: str | None = None,
    lon_dim: str | None = None,
    bbox: BoundingBox | None = None,
) -> int | None:
    """Exact row count for [start, end], optionally clipped to a bbox."""
    time_filter = _live_time_filter(dataset, start, end, time_dim, time_column)
    if time_filter is None:
        return None
    if bbox is not None and lat_dim and lon_dim:
        time_filter = time_filter & _spatial_bbox_filter(lat_dim, lon_dim, bbox)
    return _count_rows_with_retry(dataset, time_filter)


def _handle_with_index(
    q: list,
    checked_date_ranges: list[dict],
    uuid: str,
    key: str,
    meta: EstimationSidecarMetadata,
    start: pd.Timestamp,
    end: pd.Timestamp,
    times_of_split: int,
    bboxes: list[BoundingBox],
) -> bool:
    """Keep / skip / split using the weekly index.

    Returns True when the range is fully handled. False means the caller
    should live-count (tail after max_date, a single day still over the
    index threshold, or an index query failure).
    """
    last_covered = index_coverage_end(meta)
    if start > last_covered:
        log.info(
            "Date range [%s → %s] is after index max_date %s; "
            "using live row count for the uncovered tail",
            start,
            end,
            meta.max_date,
        )
        return False
    if end > last_covered:
        heapq.heappush(q, (start, last_covered, times_of_split, False))
        tail_start = last_covered + pd.Timedelta(nanoseconds=1)
        if tail_start <= end:
            heapq.heappush(q, (tail_start, end, times_of_split, True))
        log.info(
            "Date range [%s → %s] straddles index max_date %s; "
            "index-counting [%s → %s] and live-counting the tail",
            start,
            end,
            meta.max_date,
            start,
            last_covered,
        )
        return True

    num_rows = count_index_rows(uuid, key, meta, start, end, bboxes=bboxes)
    if num_rows is None:
        return False
    if num_rows == 0:
        return True
    if num_rows <= PARQUET_INDEX_SUBSET_ROW_NUMBER:
        checked_date_ranges.append({"start_date": start, "end_date": end})
        return True
    if _same_utc_day(start, end):
        log.info(
            "Index count %s for single day [%s → %s] exceeds %s; "
            "falling back to live row count",
            num_rows,
            start,
            end,
            PARQUET_INDEX_SUBSET_ROW_NUMBER,
        )
        return False

    _enqueue_split(
        q,
        start,
        end,
        times_of_split,
        num_rows,
        PARQUET_INDEX_SUBSET_ROW_NUMBER,
        False,
        checked_date_ranges,
        split_fn=_split_on_utc_day_boundary,
    )
    return True


def check_rows_with_date_range(
    api: BaseAPI,
    uuid: str,
    key: str,
    ds: ParquetDataSource,
    date_ranges: list[dict],
    polygon=None,
) -> list[dict]:
    """Split parquet date ranges so each chunk stays under the row cap.

    Prefers the weekly estimation index (DuckDB SUM over a few-MB file). Index
    zeros are trusted only inside the covered date range; the tail after
    max_date and any single day still over the (tighter) index threshold fall
    back to a live count. A polygon's bounding box prunes the count so a
    regional request is not split as if it were global.

    Live counts use the 9144 time-column filter (string columns cannot go
    through create_time_filter) plus optional lat/lon bbox.
    """
    if ".parquet" not in ds.dname:
        return date_ranges

    dataset = ds.dataset
    checked_date_ranges = []
    q = []

    time_dim = api.require_time_column(uuid=uuid, key=key)
    time_column = resolve_time_column(dataset, time_dim)

    bbox = bbox_of(polygon) if polygon is not None else None
    lat_dim = lon_dim = None
    if bbox is not None:
        mapped = api.map_column_names(
            uuid=uuid,
            key=key,
            columns=[STR_LATITUDE_UPPER_CASE, STR_LONGITUDE_UPPER_CASE],
        )
        if mapped and len(mapped) >= 2:
            lat_dim, lon_dim = mapped[0], mapped[1]
        else:
            log.warning(
                "Could not map lat/lon for %s/%s; row-count split ignores polygon",
                uuid,
                key,
            )
            bbox = None
    bboxes = [bbox] if bbox is not None else []

    index_meta = sidecar_for_row_counts(api, uuid, key)
    if index_meta is not None and not index_meta.has_time:
        log.info(
            "estimation index for %s/%s is timeless; using live row counts",
            uuid,
            key,
        )
        index_meta = None
    if index_meta is not None:
        log.info(
            "using estimation index for parquet row-count splits on %s/%s%s",
            uuid,
            key,
            (
                f" (bbox lon[{bbox.min_lon}, {bbox.max_lon}] "
                f"lat[{bbox.min_lat}, {bbox.max_lat}])"
                if bbox is not None
                else ""
            ),
        )

    for date_range in date_ranges:
        month_start = ensure_timezone(pd.Timestamp(date_range["start_date"]))
        month_end = ensure_timezone(pd.Timestamp(date_range["end_date"]))
        if month_end < month_start:
            continue
        heapq.heappush(q, (month_start, month_end, 0, False))

    while q:
        start, end, times_of_split, force_live = heapq.heappop(q)
        if times_of_split >= MAX_PARQUET_SPLIT:
            checked_date_ranges.append({"start_date": start, "end_date": end})
            continue

        if index_meta is not None and not force_live:
            if _handle_with_index(
                q,
                checked_date_ranges,
                uuid,
                key,
                index_meta,
                start,
                end,
                times_of_split,
                bboxes,
            ):
                continue

        num_rows = _live_count_rows(
            dataset, start, end, time_dim, time_column, lat_dim, lon_dim, bbox
        )
        if num_rows is None or num_rows == 0:
            continue
        if num_rows <= PARQUET_SUBSET_ROW_NUMBER:
            checked_date_ranges.append({"start_date": start, "end_date": end})
        else:
            _enqueue_split(
                q,
                start,
                end,
                times_of_split,
                num_rows,
                PARQUET_SUBSET_ROW_NUMBER,
                True,
                checked_date_ranges,
            )

    return checked_date_ranges


def create_customised_time_filter(
    dataset: ds.Dataset,
    start: pd.Timestamp,
    end: pd.Timestamp,
    time_varname: str | None = None,
    time_column: TimeColumn | None = None,
) -> ds.Expression:
    """
    Creates a time filter using actual dataset temporal extent instead of partition boundaries.

    The original create_time_filter() validates against partition boundaries, which may be
    more restrictive and ignore data less than the actual data range but larger than the partition boundaries.
    This function validates against the real data temporal extent and create a time filter within the actual temporal range.

    Args:
        dataset: PyArrow dataset object
        start: Query start timestamp
        end: Query end timestamp
        time_varname: time variable name (e.g., "JULD", "TIME", "detection_timestamp") if provided, otherwise is None

    Returns:
        PyArrow filter expression
    """
    if start.tz is None:
        start = ensure_timezone(start)
    if end.tz is None:
        end = ensure_timezone(end)

    timestamp_start, timestamp_end = get_temporal_extent(dataset, time_varname)
    timestamp_start = pd.to_datetime(timestamp_start)
    timestamp_end = pd.to_datetime(timestamp_end)

    if timestamp_start.tz is None:
        timestamp_start = ensure_timezone(timestamp_start)
    if timestamp_end.tz is None:
        timestamp_end = ensure_timezone(timestamp_end)

    if start < timestamp_start:
        start = timestamp_start
    if end > timestamp_end:
        end = timestamp_end

    if start >= end:
        raise ValueError(
            f"Invalid time range after boundary adjustment: {start} >= {end}"
        )

    start_str = to_naive_utc_string(start)
    end_str = to_naive_utc_string(end)

    partition_start, partition_end = get_timestamps_boundary_values(
        dataset, start_str, end_str
    )

    expr1 = pc.field("timestamp") >= np.int64(partition_start)
    expr2 = pc.field("timestamp") <= np.int64(partition_end)

    if time_column is None:
        time_column = resolve_time_column(dataset, time_varname)

    expr3 = pc.field(time_varname) >= time_column.to_literal(start)
    expr4 = pc.field(time_varname) <= time_column.to_literal(end)

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def trim_date_range(
    api: BaseAPI,
    uuid: str,
    key: str,
    requested_start_date: pd.Timestamp | NaTType,
    requested_end_date: pd.Timestamp | NaTType,
) -> Tuple[pd.Timestamp | None, pd.Timestamp | None]:

    log.info(f"Original date range: {requested_start_date} to {requested_end_date}")
    metadata_temporal_extent = api.get_temporal_extent(uuid=uuid, key=key)
    if (
        len(metadata_temporal_extent) != 2
        or metadata_temporal_extent[0] is None
        or metadata_temporal_extent[1] is None
    ):
        log.warning(f"Invalid metadata temporal extent: {metadata_temporal_extent}")
        return requested_start_date, requested_end_date
    metadata_start_date, metadata_end_date = metadata_temporal_extent

    metadata_start_date = metadata_start_date.tz_localize(None)
    metadata_end_date = metadata_end_date.tz_localize(None)

    if requested_start_date.tz is not None:
        requested_start_date = requested_start_date.tz_convert(pytz.UTC).tz_localize(
            None
        )

    if requested_end_date.tzinfo is not None:
        requested_end_date = requested_end_date.tz_convert(pytz.UTC).tz_localize(None)

    # Check if start and end date have overlap with the metadata time range
    if (metadata_start_date <= requested_start_date <= metadata_end_date) or (
        metadata_start_date <= requested_end_date <= metadata_end_date
    ):
        # Either start or end is within range of metadata_start or metadata_end
        if requested_start_date < metadata_start_date:
            requested_start_date = metadata_start_date
        if metadata_end_date < requested_end_date:
            requested_end_date = metadata_end_date

        log.info(f"Trimmed date range: {requested_start_date} to {requested_end_date}")
        return requested_start_date, requested_end_date
    elif (
        requested_start_date <= metadata_start_date
        and metadata_end_date <= requested_end_date
    ):
        # Request cover all the metadata range, so use metadata range due to smaller range
        return metadata_start_date, metadata_end_date
    else:
        log.info(
            f"Requested date range: {requested_start_date} to {requested_end_date} "
            f"does not overlap with metadata range: {metadata_start_date} to {metadata_end_date}"
        )
        return None, None
