import re
from typing import Tuple

import pandas as pd
from datetime import datetime, timedelta, time

import pytz
from dateutil import parser

from data_access_service import API, init_log, Config
from dateutil.relativedelta import relativedelta

from data_access_service.core.api import BaseAPI

YEAR_MONTH_DAY = "%Y-%m-%d"


# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(
    date_string: str,
    time_value=time(00, 00, 00),
    format_to_convert: str = YEAR_MONTH_DAY,
) -> datetime:
    return datetime.combine(
        datetime.strptime(date_string, format_to_convert), time_value
    )


def get_final_day_of_month_(date: datetime) -> datetime:
    next_month = date.replace(day=28) + timedelta(days=4)
    last_day_of_month = next_month - timedelta(days=next_month.day)
    return last_day_of_month


def get_first_day_of_month(date: datetime) -> datetime:
    return date.replace(day=1)


def next_month_first_day(date: datetime) -> datetime:
    return (date + relativedelta(months=1)).replace(day=1)


def get_monthly_date_range_array_from_(
    start_date: datetime, end_date: datetime
) -> list[dict]:
    """
    Split a date range into monthly intervals, returning start and end dates per month.

    Args:
        start_date (datetime): Start date of the range.
        end_date (datetime): End date of the range.

    Returns:
        list[dict]: List of dictionaries with 'start_date' and 'end_date' (as strings in 'YYYY-MM-DD').
    """
    # Check if start_date > end_date
    if start_date > end_date:
        raise ValueError("start_date should not greater then end_date")

    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    # Group by year and month, get start and end dates
    df = pd.DataFrame(date_range, columns=["date"])
    monthly_groups = df.groupby([df["date"].dt.year, df["date"].dt.month])

    # Create result list
    return [
        {
            "start_date": group["date"].min().to_pydatetime(),
            "end_date": datetime.combine(
                group["date"].max().to_pydatetime(), time(23, 59, 59)
            ),
        }
        for _, group in monthly_groups
    ]


def trim_date_range(
    api: BaseAPI,
    uuid: str,
    requested_start_date: datetime,
    requested_end_date: datetime,
) -> (datetime | None, datetime | None):
    log = init_log(Config.get_config())

    log.info(f"Original date range: {requested_start_date} to {requested_end_date}")
    metadata_temporal_extent = api.get_temporal_extent(uuid=uuid)
    if len(metadata_temporal_extent) != 2:
        log.warning(f"Invalid metadata temporal extent: {metadata_temporal_extent}")
        return requested_start_date, requested_end_date

    metadata_start_date, metadata_end_date = metadata_temporal_extent

    metadata_start_date = metadata_start_date.replace(tzinfo=None)
    metadata_end_date = metadata_end_date.replace(tzinfo=None)

    if requested_start_date.tzinfo is not None:
        requested_start_date = requested_start_date.astimezone(pytz.UTC).replace(
            tzinfo=None
        )

    if requested_end_date.tzinfo is not None:
        requested_end_date = requested_end_date.astimezone(pytz.UTC).replace(
            tzinfo=None
        )

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
    year_month = parser.parse(year_month_str)
    start_date = year_month.replace(day=1, hour=0, minute=0, second=0)
    end_date = get_final_day_of_month_(start_date).replace(
        hour=23, minute=59, second=59
    )

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


def supply_day(start_date_str: str, end_date_str: str) -> Tuple[datetime, datetime]:
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
        raise ValueError(
            "Date strings are not in format 'MM-yyyy'. please use other function or update this one"
        )

    start_date = parser.parse(start_date_str)
    end_date = parser.parse(end_date_str)

    start_date = start_date.replace(day=1, hour=0, minute=0, second=0)
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
