import logging
import re
import time

import pandas as pd
import pytz

from pandas import Timestamp
from functools import wraps
from typing import Tuple
from datetime import datetime
from inspect import iscoroutinefunction

from dateutil.relativedelta import relativedelta
from pandas._libs import NaTType

from data_access_service.models.subset_request import NON_SPECIFIED

YEAR_MONTH_DAY = "%Y-%m-%d"
YEAR_MONTH_DAY_TIME_NANO = "%Y-%m-%d %H:%M:%S.fffffffff"

# %z do not produce Z for +0000, %z just add the offset value which is fine
# for client, however if you prefer to have Z the please replace the string
# output manually
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


log = logging.getLogger(__name__)


# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(
    date_string: str, format_to_convert: str | None = None, time_zone: str = pytz.UTC
) -> pd.Timestamp | NaTType:
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


def start_of_day_nano(ts: pd.Timestamp) -> pd.Timestamp:
    """Floor a timestamp to the first nanosecond of its day (00:00:00.000000000)."""
    return ts.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        # pyrefly: ignore [unexpected-keyword]
        nanosecond=0,
    )


def end_of_day_nano(ts: pd.Timestamp) -> pd.Timestamp:
    """Ceil a timestamp to the final nanosecond of its day (23:59:59.999999999)."""
    # a coarser-resolution timestamp (e.g. "us") silently drops the
    # nanosecond=999 in replace(), so force nanosecond resolution first
    return ts.as_unit("ns").replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        # pyrefly: ignore [unexpected-keyword]
        nanosecond=999,
    )


def get_final_day_of_month_(date: pd.Timestamp) -> pd.Timestamp:
    if date.tz is None:
        date = date.tz_localize(pytz.UTC)
    return end_of_day_nano(date + pd.offsets.MonthEnd(0))


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


def ensure_timezone(dt: pd.Timestamp | NaTType) -> pd.Timestamp | NaTType:
    """
    Check if datetime has timezone info; if not, assume UTC.

    Args:
        dt: Input datetime object

    Returns:
        Datetime object with timezone info (UTC if none was present)
    """
    if dt.tz is None and not isinstance(dt, NaTType):
        return dt.tz_localize(pytz.UTC)
    return dt


def to_naive_utc(ts: pd.Timestamp | None) -> pd.Timestamp | None:
    """Convert a timestamp to naive UTC for slicing the zarr time coordinate, which cannot be compared against timezone-aware values.None passes through so an open slice stays open."""
    if ts is None:
        return None
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def split_date_range_binary(
    start_date: Timestamp, end_date: Timestamp
) -> tuple[Timestamp, Timestamp | NaTType, Timestamp | NaTType, Timestamp]:
    """
    Binary-split a date range into two adjacent, non-overlapping inclusive halves.

    Filters treat both ends as inclusive, so the split uses a 1ns gap at the mid
    point: left is [start, mid_exclusive_end] and right is [right_start, end],
    with mid_exclusive_end + 1ns == right_start. Together they cover [start, end]
    without sharing any timestamp.

    Args:
        start_date: Inclusive start of the range (UTC).
        end_date: Inclusive end of the range (UTC).

    Returns:
        (left_start, left_end, right_start, right_end)

    Raises:
        ValueError: If the range is too short to split into two non-empty halves.
    """
    if not isinstance(start_date, pd.Timestamp):
        start_date = pd.Timestamp(start_date)
    if not isinstance(end_date, pd.Timestamp):
        end_date = pd.Timestamp(end_date)

    start_date = ensure_timezone(start_date)
    end_date = ensure_timezone(end_date)

    if isinstance(start_date, NaTType):
        raise ValueError(f"Invalid start_date of type NaTType")

    if isinstance(end_date, NaTType):
        raise ValueError(f"Invalid end_date of type NaTType")

    if end_date < start_date:
        raise ValueError(f"Invalid range: end {end_date} is before start {start_date}")

    # Need at least 2 distinct nanosecond ticks so each half is non-empty.
    # Work in integer nanoseconds so we never hit pandas' Timestamp|NaTType
    # arithmetic stubs (Timestamp ± Timedelta is typed as possibly NaT).
    start_ns = int(start_date.value)
    end_ns = int(end_date.value)
    duration_ns = end_ns - start_ns
    if duration_ns < 1:
        raise ValueError(
            f"Range too short to split without overlap: {start_date} to {end_date}"
        )

    # right_start is the first tick of the right half (ceiling of midpoint).
    mid_offset_ns = (duration_ns + 1) // 2
    right_start_ns = start_ns + mid_offset_ns
    left_end_ns = right_start_ns - 1
    tz = start_date.tz

    right_start = pd.Timestamp(right_start_ns, unit="ns", tz=tz)
    left_end = pd.Timestamp(left_end_ns, unit="ns", tz=tz)

    if left_end < start_date or right_start > end_date:
        raise ValueError(
            f"Range too short to split without overlap: {start_date} to {end_date}"
        )

    return start_date, left_end, right_start, end_date


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


def resolve_non_specified_dates(start_date: str, end_date: str) -> Tuple[str, str]:
    """
    Resolve non-specified start and end dates to default values.
    defaults: 1970-01-01 for an open start and today for an open end.
    """
    if start_date == NON_SPECIFIED:
        start_date = "1970-01-01"
    if end_date == NON_SPECIFIED:
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    return start_date, end_date


def supply_day_with_nano_precision(
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
        return parse_date(start_date_str), end_of_day_nano(parse_date(end_date_str))

    start_date = parse_date(start_date_str, format_to_convert="%m-%Y")
    end_date = parse_date(end_date_str, format_to_convert="%m-%Y")

    start_date = get_first_day_of_month(start_date)
    end_date = end_of_day_nano(get_final_day_of_month_(end_date))

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
        for i in range(0, len(months), month_count_per_job):
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


def time_it(func):
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        log.info(f"[{func.__name__}] took {end - start:.6f} seconds.")
        return result

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        log.info(f"[{func.__name__}] took {end - start:.6f} seconds.")
        return result

    return async_wrapper if iscoroutinefunction(func) else sync_wrapper
