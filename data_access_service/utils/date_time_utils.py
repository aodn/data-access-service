import datetime

from dateutil import parser

# parse all common format of date string into given format, such as "%Y-%m-%d"
def parse_date(date_string: str, format_to_convert: str) -> datetime:
    parsed_date = parser.parse(date_string, default=datetime.datetime(1,1,1))
    return parsed_date.strftime(format_to_convert)
