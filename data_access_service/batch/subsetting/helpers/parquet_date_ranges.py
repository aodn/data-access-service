"""Date-range preparation for the PARQUET batch download."""

import heapq
import logging
import numpy as np
import pandas as pd
import pytz

from typing import Tuple
import pyarrow.dataset as ds
from pyarrow import compute as pc
from tenacity import retry, stop_after_attempt, wait_exponential

from aodn_cloud_optimised.lib.DataQuery import (
    DateOutOfRangeError,
    get_temporal_extent,
    get_timestamps_boundary_values,
    create_time_filter,
    ParquetDataSource,
)

from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    MAX_PARQUET_SPLIT,
    PARQUET_SUBSET_ROW_NUMBER,
    STR_TIME_UPPER_CASE,
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
    return dataset.count_rows(filter=time_filter)


def check_rows_with_date_range(
    api: BaseAPI, uuid: str, key: str, ds: ParquetDataSource, date_ranges: list[dict]
) -> list[dict]:
    """
    Count number of rows with specific monthly range. ignore bbox.
    If rows number exceeds PARQUET_SUBSET_ROW_NUMBER, split this date range with binary division, until rows number
    under the safe threshold.
    If rows number is 0, remove this date range from the list of date_ranges so that to skip further querying data.
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
    checked_date_ranges = []
    q = []

    time_dim = api.map_column_names(uuid=uuid, key=key, columns=[STR_TIME_UPPER_CASE])[
        0
    ]

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

        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        try:
            time_filter = create_time_filter(
                dataset=dataset,
                date_start=start_str,
                date_end=end_str,
                time_varname=time_dim,
            )
        except DateOutOfRangeError as e:
            # create_time_filter validates against partition/temporal bounds and can
            # raise false positives; fall back to a filter clamped to real extent.
            # Import note: catch DataQuery.DateOutOfRangeError (what create_time_filter
            # raises) — lib.exceptions.DateOutOfRangeError is a separate class.
            log.info(
                "create_time_filter out of range for %s to %s (%s); "
                "trying customised time filter",
                start_str,
                end_str,
                e,
            )
            try:
                time_filter = create_customised_time_filter(
                    dataset=dataset, start=start, end=end, time_varname=time_dim
                )
            except ValueError as e2:
                # Fully non-overlapping after clamp (e.g. query after dataset end).
                log.info(
                    "Skipping date range %s to %s: no overlap with dataset extent (%s)",
                    start,
                    end,
                    e2,
                )
                continue
        num_rows = _count_rows_with_retry(dataset, time_filter)

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
            log.info(f"Splitting range {start} to {end} (rows: {num_rows})")
            try:
                split_start, split_mid, split_end = split_date_range_binary(start, end)
                heapq.heappush(q, (split_start, split_mid, times_of_split + 1))
                heapq.heappush(q, (split_mid, split_end, times_of_split + 1))

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

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    partition_start, partition_end = get_timestamps_boundary_values(
        dataset, start_str, end_str
    )

    expr1 = pc.field("timestamp") >= np.int64(partition_start)
    expr2 = pc.field("timestamp") <= np.int64(partition_end)

    start_naive = start.tz_localize(None) if start.tz is not None else start
    end_naive = end.tz_localize(None) if end.tz is not None else end

    expr3 = pc.field(time_varname) >= start_naive
    expr4 = pc.field(time_varname) <= end_naive

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def trim_date_range(
    api: BaseAPI,
    uuid: str,
    key: str,
    requested_start_date: pd.Timestamp,
    requested_end_date: pd.Timestamp,
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
