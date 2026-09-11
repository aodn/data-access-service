"""Date-range preparation for the PARQUET batch download."""

import heapq
import logging
import numpy as np
import pandas as pd
import pytz

from typing import Tuple
import pyarrow.dataset as ds
from pandas._libs import NaTType
from tenacity import retry, stop_after_attempt, wait_exponential

from pyarrow import compute as pc

from aodn_cloud_optimised.lib.DataQuery import (
    get_temporal_extent,
    ParquetDataSource,
    query_unique_value,
)

from data_access_service.utils.time_column_utils import (
    TimeColumn,
    build_time_filter,
    resolve_time_column,
)
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    MAX_PARQUET_SPLIT,
    PARQUET_SUBSET_ROW_NUMBER,
)
from data_access_service.utils.date_time_utils import (
    ensure_timezone,
    split_date_range_binary,
)

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
    if time_filter is None:
        return dataset.count_rows()
    return dataset.count_rows(filter=time_filter)


def _row_level_time_filter(
    time_column: TimeColumn, start: pd.Timestamp, end: pd.Timestamp
) -> ds.Expression:
    return (pc.field(time_column.name) >= time_column.to_literal(start)) & (
        pc.field(time_column.name) <= time_column.to_literal(end)
    )


def _count_rows_for_range(
    dataset, time_column: TimeColumn, start: pd.Timestamp, end: pd.Timestamp
) -> int:
    """Count rows in `[start, end]` from the time column. Used when the dataset
    has no hive `timestamp` partition key."""
    return _count_rows_with_retry(
        dataset, _row_level_time_filter(time_column, start, end)
    )


def _unix_seconds(ts) -> int:
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _timestamp_partition_buckets(dataset) -> np.ndarray | None:
    """Sorted hive `timestamp` bucket starts, or None if the dataset has none.

    Path-based: ``query_unique_value`` reads directory names, not file bodies.
    """
    if "timestamp" not in dataset.schema.names:
        return None
    try:
        unique = query_unique_value(dataset, "timestamp")
        if not unique:
            return None
        buckets = np.array([np.int64(value) for value in unique])
        buckets.sort()
        return buckets
    except Exception as e:
        log.warning("timestamp partition keys unreadable: %s", e)
        return None


def _window_overlaps_timestamp_buckets(start, end, buckets: np.ndarray) -> bool:
    """Whether `[start, end]` can hold rows in any hive `timestamp` bucket.

    Bucket ``i`` covers ``[buckets[i], buckets[i+1])``; the last bucket is
    unbounded on the right because the key is only the bin start.
    """
    window_start = _unix_seconds(start)
    window_end = _unix_seconds(end)
    if window_end < int(buckets[0]):
        return False
    for i, bucket in enumerate(buckets):
        bucket_end = int(buckets[i + 1]) if i + 1 < len(buckets) else None
        if bucket_end is None:
            return window_end >= int(bucket)
        if window_start < bucket_end and window_end >= int(bucket):
            return True
    return False


def _select_ranges_by_timestamp_buckets(
    date_ranges: list[dict], buckets: np.ndarray
) -> list[dict]:
    """Keep windows that overlap a hive `timestamp` bucket. No file is opened."""
    selected = []
    for date_range in date_ranges:
        start, end = date_range["start_date"], date_range["end_date"]
        if end < start:
            continue
        if _window_overlaps_timestamp_buckets(start, end, buckets):
            selected.append({"start_date": start, "end_date": end})
    return selected


def check_rows_with_date_range(
    api: BaseAPI, uuid: str, key: str, ds: ParquetDataSource, date_ranges: list[dict]
) -> list[dict]:
    """
    Prepare parquet monthly windows for download.

    When the dataset is hive-partitioned by ``timestamp``, selection uses the
    partition directory names only (no footer reads, no time-column scan).
    Windows that cannot overlap a bucket are dropped; the rest are kept as-is.
    The caller already clips the request to the metadata temporal extent.

    Without a ``timestamp`` key, count rows on the time column. If a window
    exceeds PARQUET_SUBSET_ROW_NUMBER, split it by binary division until each
    piece is under the threshold. A window with 0 rows is dropped.
    Args:
        api: BaseAPI instance for column name mapping
        uuid: Dataset UUID for metadata lookup
        key: Metadata key for column mapping
        ds: DataSource fetched from cloud optimised library
        date_ranges: List of monthly intervals as dictionaries with 'start_date' and 'end_date' as UTC timestamps in
                    'YYYY-MM-DD HH:MM:SS.fffffffff+00:00' format.
    Returns:
        List[dict]: List of dictionaries with 'start_date' and 'end_date' as UTC timestamps in
                    'YYYY-MM-DD HH:MM:SS.fffffffff+00:00' format with row number check.
    """
    # apply on parquet dataset only
    if ".parquet" not in ds.dname:
        return date_ranges

    dataset = ds.dataset

    buckets = _timestamp_partition_buckets(dataset)
    if buckets is not None:
        selected = _select_ranges_by_timestamp_buckets(date_ranges, buckets)
        log.info(
            "Selected %s of %s date window(s) from %s timestamp partition "
            "keys (no row scan)",
            len(selected),
            len(date_ranges),
            len(buckets),
        )
        return selected

    checked_date_ranges = []
    q = []

    time_dim = api.require_time_column(uuid=uuid, key=key)
    # Resolve once: a string column needs a one-off format check, and the
    # literal in the row-level filter must match the stored type.
    time_column = resolve_time_column(dataset, time_dim)

    # Go through monthly interval
    for date_range in date_ranges:
        month_start, month_end = date_range["start_date"], date_range["end_date"]
        if month_end < month_start:
            continue
        heapq.heappush(q, (month_start, month_end, 0))

    # check row count
    while q:
        start, end, times_of_split = heapq.heappop(q)
        if times_of_split >= MAX_PARQUET_SPLIT:
            checked_date_ranges.append({"start_date": start, "end_date": end})
            continue

        num_rows = _count_rows_for_range(dataset, time_column, start, end)

        if num_rows == 0:
            # skip the date range if no data in this range
            continue
        elif num_rows <= PARQUET_SUBSET_ROW_NUMBER:
            checked_date_ranges.append(
                {
                    "start_date": start,
                    "end_date": end,
                }
            )
        else:
            try:
                left_start, left_end, right_start, right_end = split_date_range_binary(
                    start, end
                )
                # Parent is discarded: only the two non-overlapping halves are
                # re-queued. Log makes that replacement explicit so nested split
                # lines are not read as re-processing the same parent range.
                log.info(
                    "Range too large (%s rows > limit %s); discarding parent "
                    "[%s → %s] and enqueueing non-overlapping halves "
                    "[%s → %s] and [%s → %s] (split depth %s → %s)",
                    num_rows,
                    PARQUET_SUBSET_ROW_NUMBER,
                    start,
                    end,
                    left_start,
                    left_end,
                    right_start,
                    right_end,
                    times_of_split,
                    times_of_split + 1,
                )
                heapq.heappush(q, (left_start, left_end, times_of_split + 1))
                heapq.heappush(q, (right_start, right_end, times_of_split + 1))

            except Exception as e:
                log.warning(f"Could not split range {start} to {end}: {e}")
                checked_date_ranges.append(
                    {
                        "start_date": start,
                        "end_date": end,
                    }
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

    if time_column is None:
        time_column = resolve_time_column(dataset, time_varname)

    return build_time_filter(dataset, time_column, start, end)


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
