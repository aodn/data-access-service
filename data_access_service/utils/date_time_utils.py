import datetime
from typing import List

from dateutil import parser
from dateutil.relativedelta import relativedelta

from data_access_service.models.date_range import DateRange

YEAR_MONTH_DAY = "%Y-%m-%d"


# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(date_string: str, format_to_convert: str) -> datetime:
    parsed_date = parser.parse(date_string, default=datetime.datetime(1, 1, 1))
    return parsed_date.strftime(format_to_convert)


def get_final_day_of_(date: datetime) -> datetime:
    next_month = date.replace(day=28) + datetime.timedelta(days=4)
    last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
    return last_day_of_month


def get_first_day_of_(date: datetime) -> datetime:
    return date.replace(day=1)


def next_month_first_day(date: datetime) -> datetime:
    return (date + relativedelta(months=1)).replace(day=1)


def is_same_year_month(date1: datetime, date2: datetime) -> bool:
    return date1.year == date2.year and date1.month == date2.month


def get_yearly_date_range_array_from_(
    start_date: datetime, end_date: datetime
) -> List[DateRange]:
    dates = []
    if start_date.year == end_date.year:
        return [DateRange(start_date, end_date)]

    current_year_start = start_date.replace(month=1, day=1)

    dates.append(DateRange(start_date, datetime.datetime(start_date.year, 12, 31)))
    current_year_start = current_year_start.replace(year=current_year_start.year + 1)

    while current_year_start.year <= end_date.year:
        if current_year_start.year == end_date.year:
            dates.append(DateRange(current_year_start, end_date))
            break
        else:
            dates.append(
                DateRange(
                    current_year_start,
                    datetime.datetime(current_year_start.year, 12, 31),
                )
            )

        # move to the first day of the next year
        current_year_start = current_year_start.replace(
            year=current_year_start.year + 1
        )

    return dates


def get_monthly_date_range_array_from_(
    start_date: datetime, end_date: datetime
) -> List[DateRange]:
    dates = []
    if start_date.year == end_date.year and start_date.month == end_date.month:
        return [DateRange(start_date, end_date)]

    current_month_start = start_date.replace(day=1)

    dates.append(DateRange(start_date, get_final_day_of_(start_date)))
    current_month_start = next_month_first_day(current_month_start)

    while current_month_start.year < end_date.year or (
        current_month_start.year == end_date.year
        and current_month_start.month <= end_date.month
    ):
        if (
            current_month_start.year == end_date.year
            and current_month_start.month == end_date.month
        ):
            dates.append(DateRange(current_month_start, end_date))
            break
        else:
            dates.append(
                DateRange(current_month_start, get_final_day_of_(current_month_start))
            )

        # move to the first day of the next month
        current_month_start = next_month_first_day(current_month_start)

    return dates
