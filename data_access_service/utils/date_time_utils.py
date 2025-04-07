import datetime
from typing import List

from dateutil import parser
from dateutil.relativedelta import relativedelta

from data_access_service.utils.date_range import DateRange

YEAR_MONTH_DAY = "%Y-%m-%d"

# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(date_string: str, format_to_convert: str) -> datetime:
    parsed_date = parser.parse(date_string, default=datetime.datetime(1, 1, 1))
    return parsed_date.strftime(format_to_convert)

def get_final_day_of_(date: datetime) -> datetime:
    next_month = date.replace(day=28) + datetime.timedelta(days=4)
    last_day_of_month = next_month - datetime.timedelta(days=next_month.day)
    return last_day_of_month

def get_first_day_of (date:datetime) -> datetime:
    return date.replace(day=1)

def next_month_first_day(date: datetime) -> datetime:
    return (date + relativedelta(months=1)).replace(day=1)

def is_same_year_month(date1: datetime, date2: datetime) -> bool:
    return date1.year == date2.year and date1.month == date2.month

def get_date_range_array_from_(start_date: datetime, end_date: datetime) -> List[DateRange]:
    dates = []
    if is_same_year_month(start_date, end_date):
        return [DateRange(start_date, end_date)]

    current_year_month = start_date.replace(day=1)

    dates.append(DateRange(start_date, get_final_day_of_(start_date)))
    current_year_month = next_month_first_day(current_year_month)

    while current_year_month <= end_date:
        if is_same_year_month(current_year_month, end_date):
            dates.append(DateRange(current_year_month, end_date))
            break
        else:
            dates.append(DateRange(current_year_month, get_final_day_of_(current_year_month)))

        # move to the first day of the next month
        current_year_month = next_month_first_day(current_year_month)

    return dates

