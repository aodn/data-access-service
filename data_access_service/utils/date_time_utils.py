import re
import pandas as pd
import pytz

from typing import Tuple
from datetime import datetime, timedelta, time
from dateutil import parser
from data_access_service import init_log, Config
from dateutil.relativedelta import relativedelta
from data_access_service.core.api import BaseAPI

YEAR_MONTH_DAY = "%Y-%m-%d"

# %z do not produce Z for +0000, %z just add the offset value which is fine
# for client, however if you prefer to have Z the please replace the string
# output manually
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


config: Config = Config.get_config()
log = init_log(config)


# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(
    date_string: str, format_to_convert: str = YEAR_MONTH_DAY, time_zone: str = pytz.UTC
) -> pd.Timestamp:
    return pd.to_datetime(date_string, format=format_to_convert).tz_localize(time_zone)


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
    first_day = date + pd.offsets.MonthBegin(0)
    return first_day.normalize().tz_convert(pytz.UTC)  # Set time to 00:00:00


def next_month_first_day(date: pd.Timestamp) -> pd.Timestamp:
    return get_final_day_of_month_(date) + pd.offsets.Day(1)


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


def get_monthly_date_range_array_from_(
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
            result.append(
                {
                    "start_date": start,
                    "end_date": d,
                }
            )
            # The next start time will be 1 nanosecond more than the end_date
            start = d + pd.offsets.Nano(1)

    # Edge case where you have start but no end
    if start < end_date:
        result.append(
            {
                "start_date": start,
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
    if len(metadata_temporal_extent) != 2:
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
            requested_end_date = datetime.combine(
                metadata_end_date, requested_end_date.time()
            )

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
    start_date: datetime,
    end_date: datetime,
    month_count_per_job: int,
) -> dict:
    date_ranges = {}
    index = 0
    # to make sure the start date is at the very beginning of the month
    current_start_date = start_date.replace(hour=0, minute=0, second=0)
    while current_start_date <= end_date:
        current_end_date = get_final_day_of_month_(
            current_start_date + relativedelta(months=(month_count_per_job - 1))
        ).replace(hour=23, minute=59, second=59)

        # Check if the end date exceeds the original end date
        if current_end_date > end_date:
            current_end_date = end_date.replace(hour=23, minute=59, second=59)
        date_ranges[index] = [
            current_start_date.strftime("%Y-%m-%d"),
            current_end_date.strftime("%Y-%m-%d"),
        ]
        index += 1
        current_start_date = current_end_date + relativedelta(seconds=1)
    return date_ranges
