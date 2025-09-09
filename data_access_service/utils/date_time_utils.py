import re
import pandas as pd
import pytz
import pyarrow
from pyarrow import compute as pc
import numpy as np
import heapq
from pandas import Timestamp

from typing import Tuple
from datetime import datetime

from aodn_cloud_optimised.lib.DataQuery import (
    DataSource,
    get_temporal_extent,
    get_timestamps_boundary_values,
    create_time_filter,
)

from data_access_service import init_log, Config
from dateutil.relativedelta import relativedelta
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    PARQUET_SUBSET_ROW_NUMBER,
    MAX_PARQUET_SPLIT,
)

YEAR_MONTH_DAY = "%Y-%m-%d"
YEAR_MONTH_DAY_TIME_NANO = "%Y-%m-%d %H:%M:%S.fffffffff"

# %z do not produce Z for +0000, %z just add the offset value which is fine
# for client, however if you prefer to have Z the please replace the string
# output manually
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


config: Config = Config.get_config()
log = init_log(config)


# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(
    date_string: str, format_to_convert: str = None, time_zone: str = pytz.UTC
) -> pd.Timestamp:
    if format_to_convert is None:
        return pd.Timestamp(date_string).tz_localize(time_zone)
    else:
        # Custom format
        ts = pd.to_datetime(date_string, format=format_to_convert)
        # Extract nanoseconds if present
        if "%f" in format_to_convert:
            frac_part = date_string.split(".")[-1].split("+")[0]
            if len(frac_part) > 6:
                nano_str = frac_part[6:9]
                nanosec = int(nano_str) if nano_str else 0
                ts = ts + pd.Timedelta(nanoseconds=nanosec)
        return ts.tz_localize(time_zone)


def get_final_day_of_month_(date: pd.Timestamp) -> pd.Timestamp:
    if date.tz is None:
        date = date.tz_localize(pytz.UTC)

    last_day = date + pd.offsets.MonthEnd(0)
    # Replace do not set nano sec correctly
    last_day = last_day.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
    )
    return last_day + pd.offsets.Nano(999)


def get_first_day_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """
    Find first day of month, do not care about the timezone and time
    :param date:
    :return:
    """
    first_day = date + pd.offsets.MonthBegin(0)
    return first_day.normalize()


def next_month_first_day(date: pd.Timestamp) -> pd.Timestamp:
    first_day = get_final_day_of_month_(date) + pd.offsets.Day(1)
    return pd.Timestamp(
        year=first_day.year, month=first_day.month, day=first_day.day, tz=first_day.tz
    )


def check_rows_with_date_range(ds: DataSource, date_ranges: list[dict]) -> list[dict]:
    """
    Count number of rows with specific monthly range. ignore bbox.
    If rows number exceeds PARQUET_SUBSET_ROW_NUMBER, split this date range with binary division, until rows number
    under the safe threshold.
    If rows number is 0, remove this date range from the list of date_ranges so that to skip further querying data.
    Args:
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

    # go through monthly interval
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
                dataset, date_start=start_str, date_end=end_str
            )
        except ValueError as e:
            if "is out of range of dataset." in str(e):
                try:
                    time_filter = create_customised_time_filter(
                        dataset, start=start, end=end
                    )
                except ValueError as e:
                    log.warning(
                        f"Warning: Could not create time filter for range {start} to {end}: {e}"
                    )
                    continue
            else:
                raise ValueError(f"Could not create time filter: {e}")

        try:
            num_rows = dataset.count_rows(filter=time_filter)
        except Exception as e:
            log.warning(
                f"Warning: Could not count rows for range {start} to {end}: {e}"
            )
            continue

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
                log.warning(f"Warning: Could not split range {start} to {end}: {e}")
                checked_date_ranges.append(
                    {
                        "start_date": start,
                        "end_date": end,
                    }
                )

    return checked_date_ranges


def create_customised_time_filter(
    dataset, start: pd.Timestamp, end: pd.Timestamp
) -> pyarrow.dataset.Expression:
    """
    Creates a time filter using actual dataset temporal extent instead of partition boundaries.

    The original create_time_filter() validates against partition boundaries, which may be
    more restrictive and ignore data less than the actual data range but larger than the partition boundaries.
    This function validates against the real data temporal extent and create a time filter within the actual temporal range.

    Args:
        dataset: PyArrow dataset object
        start: Query start timestamp
        end: Query end timestamp

    Returns:
        PyArrow filter expression
    """
    if start.tz is None:
        start = ensure_timezone(start)
    if end.tz is None:
        end = ensure_timezone(end)

    timestamp_start, timestamp_end = get_temporal_extent(dataset)
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

    time_varname = "TIME"
    if "TIME" in dataset.schema.names:
        time_varname = "TIME"
    elif "JULD" in dataset.schema.names:
        time_varname = "JULD"
    elif "detection_timestamp" in dataset.schema.names:
        time_varname = "detection_timestamp"

    start_naive = start.tz_localize(None) if start.tz is not None else start
    end_naive = end.tz_localize(None) if end.tz is not None else end

    expr3 = pc.field(time_varname) >= start_naive
    expr4 = pc.field(time_varname) <= end_naive

    expression = expr1 & expr2 & expr3 & expr4
    return expression


def ensure_timezone(dt: pd.Timestamp) -> pd.Timestamp:
    """
    Check if datetime has timezone info; if not, assume UTC.

    Args:
        dt: Input datetime object

    Returns:
        Datetime object with timezone info (UTC if none was present)
    """
    if dt.tz is None:
        return dt.tz_localize(pytz.UTC)
    return dt


def split_date_range_binary(
    start_date: Timestamp, end_date: Timestamp
) -> tuple[Timestamp, Timestamp, Timestamp]:
    """
    A basic binary division to split date range
    Args:
        start_date: The start date of the range. UTC string in 'YYYY-MM-DD HH:MM:SS.fffffffff+00:00' format.
        end_date: The end date of the range. UTC string in 'YYYY-MM-DD HH:MM:SS.fffffffff+00:00' format.
    Returns:
        tuple[str, str, str] The start date, mid date, and end date of the date range.
    """
    if not isinstance(start_date, pd.Timestamp):
        start_date = pd.Timestamp(start_date)
    if not isinstance(end_date, pd.Timestamp):
        end_date = pd.Timestamp(end_date)

    duration_ns = (end_date - start_date).total_seconds() * 1e9
    mid_ns = duration_ns / 2

    mid_date = start_date + pd.Timedelta(nanoseconds=mid_ns)
    # make sure the time zone is attached
    start_date = ensure_timezone(start_date)
    mid_date = ensure_timezone(mid_date)
    end_date = ensure_timezone(end_date)

    return start_date, mid_date, end_date


def get_monthly_utc_date_range_array_from_(
    start_date: pd.Timestamp, end_date: pd.Timestamp
) -> list[dict]:
    """
    Split a date range into monthly intervals, preserving start_date and using exact end_date for the last month.

    Args:
        start_date (pd.Timestamp): Start date with nanosecond precision.
        end_date (pd.Timestamp): End date with nanosecond precision.

    Returns:
        list[dict]: List of dictionaries with 'start_date' and 'end_date' as UTC strings in
                    'YYYY-MM-DD HH:MM:SS.fffffffff+00:00' format.
    """
    # Check if start_date > end_date
    if start_date > end_date:
        raise ValueError("start_date should not be greater than end_date")

    # Ensure naive timestamps for consistency
    start_date = (
        start_date.tz_convert(pytz.UTC)
        if start_date.tz is not None
        else start_date.tz_localize(pytz.UTC)
    )
    end_date = (
        end_date.tz_convert(pytz.UTC)
        if end_date.tz is not None
        else end_date.tz_localize(pytz.UTC)
    )

    # Handle the case where start_date == end_date
    if start_date == end_date:
        return [{"start_date": start_date, "end_date": end_date}]

    # Generate date range, excluding end_date
    date_range = pd.date_range(
        start=start_date, end=end_date, freq="D", inclusive="left"
    )
    if not date_range.is_monotonic_increasing:
        raise ValueError("Generated date range is not monotonic")

    # Create DataFrame and group by year and month
    df = pd.DataFrame({"date": date_range}).sort_values(by="date")

    # Initialize result
    result = []

    # Iterate over months from start_date to end_date
    start = None
    for d in df["date"]:
        if start is None:
            # This the first start day
            start = d
        elif d.is_month_end:
            v = pd.Timestamp(
                year=d.year,
                month=d.month,
                day=d.day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
                nanosecond=999,
                tz=pytz.UTC,
            )
            result.append(
                {
                    "start_date": start,
                    # Must set to end of time of that day
                    "end_date": v,
                }
            )
            # The next start time will be 1 nanosecond more than the end_date
            start = (v + pd.offsets.MonthBegin(1)).normalize()

    # Edge case where you have start but no end
    if start < end_date:
        result.append(
            {
                "start_date": start,
                # This one needs to follow the one from the incoming request
                "end_date": end_date,
            }
        )
    return result


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


def get_boundary_of_year_month(
    year_month_str: str,
) -> Tuple[datetime, datetime]:
    """
    Get the first and last day of the month for a given year and month.

    Args:
        year_month_str (str): Year and month in the format "YYYY-MM".

    Returns:
        Tuple[datetime, datetime]: First and last day of the month.
    """
    try:
        year_month = parse_date(year_month_str, "%Y-%m")
    except Exception as ex:
        year_month = parse_date(year_month_str, "%m-%Y")

    start_date = year_month.replace(day=1, hour=0, minute=0, second=0)
    end_date = get_final_day_of_month_(start_date)

    return start_date, end_date


def transfer_date_range_into_yearmonth(start_date: str, end_date: str) -> list[dict]:
    """
    Transfer a date range into a list of dictionaries with year and month. currently, according to the
    request from the frontend, the start & end date is in the format of "MM-yyyy"

    Args:
        start_date (str): Start date in the format "MM-yyyy".
        end_date (str): End date in the format "MM-yyyy".

    Returns:
        list[dict]: List of dictionaries with year month in "MM-yyyy" format.
    """
    start = datetime.strptime(start_date, "%m-%Y")
    end = datetime.strptime(end_date, "%m-%Y")
    result = []

    while start <= end:
        result.append(start.strftime("%m-%Y"))
        start += relativedelta(months=1)

    return result


def split_yearmonths_into_dict(yearmonths, chunk_size: int):
    """
    Split a list of yearmonths into a dictionary with chunks of a given size.

    Args:
        yearmonths (list): List of yearmonth strings.
        chunk_size (int): Size of each chunk (default is 4).

    Returns:
        dict: Dictionary where keys are indices and values are chunks of yearmonths.
    """
    result = {}
    for i in range(0, len(yearmonths), chunk_size):
        result[i // chunk_size] = yearmonths[i : i + chunk_size]
    return result


def supply_day(
    start_date_str: str, end_date_str: str
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Supply the day to the start and end date strings. if the date string is not in this format: "MM-yyyy", don't use this function

    Args:
        start_date_str (str): Start date string.
        end_date_str (str): End date string.

    Returns:
        Tuple[datetime, datetime]: Start and end dates as datetime objects.
    """
    pattern = r"^(0[1-9]|1[0-2])-\d{4}$"
    if (not re.match(pattern, start_date_str)) or (not re.match(pattern, end_date_str)):
        # currently, if no date ranges selected in frontend, the start_date & end_date will be in this format: "yyyy-MM-dd",
        # so for this case, we don't need to supply the day
        return parse_date(start_date_str), parse_date(end_date_str)

    start_date = parse_date(start_date_str, format_to_convert="%m-%Y")
    end_date = parse_date(end_date_str, format_to_convert="%m-%Y")

    start_date = get_first_day_of_month(start_date)
    end_date = get_final_day_of_month_(end_date).replace(hour=23, minute=59, second=59)

    return start_date, end_date


def split_date_range(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    month_count_per_job: int,
) -> dict:
    date_ranges = {}
    index = 0

    months: list[dict] = get_monthly_utc_date_range_array_from_(start_date, end_date)

    # Special case, if your split is too high and cannot be split we just return the start end date
    if len(months) < month_count_per_job:
        date_ranges[0] = [
            f"{months[0]['start_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{months[0]['start_date'].nanosecond:03d}",
            f"{months[-1]['end_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{months[-1]['end_date'].nanosecond:03d}",
        ]
    else:
        for i in range(0, len(months) - month_count_per_job + 1, month_count_per_job):
            window = months[i : i + month_count_per_job]
            if len(window) < month_count_per_job:
                date_ranges[index] = [
                    f"{window[0]['start_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{window[0]['start_date'].nanosecond:03d}",
                    f"{window[-1]['end_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{window[-1]['end_date'].nanosecond:03d}",
                ]
                break  # Skip incomplete windows

            date_ranges[index] = [
                f"{window[0]['start_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{window[0]['start_date'].nanosecond:03d}",
                f"{window[-1]['end_date'].strftime('%Y-%m-%d %H:%M:%S.%f')}{window[-1]['end_date'].nanosecond:03d}",
            ]
            index = index + 1

    return date_ranges
