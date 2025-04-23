# tests/utils/test_date_time_utils.py
import unittest
import datetime

from data_access_service.models.date_range import DateRange
from data_access_service.utils.date_time_utils import (
    parse_date,
    get_final_day_of_,
    next_month_first_day,
    get_yearly_date_range_array_from_,
    get_monthly_date_range_array_from_,
)


class TestDateTimeUtils(unittest.TestCase):

    def test_parse_date(self):
        date_string = "2023-10-01"
        expected_date = datetime.datetime(2023, 10, 1)
        self.assertEqual(parse_date(date_string, "%Y-%m-%d"), expected_date)

    def test_parse_date2(self):
        date_string = "2023/10/01"
        expected_date = datetime.datetime(2023, 10, 1)
        self.assertEqual(parse_date(date_string, "%Y/%m/%d"), expected_date)

    def test_parse_date3(self):
        date_string = "2023.10.01"
        expected_date = datetime.datetime(2023, 10, 1)
        self.assertEqual(parse_date(date_string, "%Y.%m.%d"), expected_date)

    def test_parse_date4(self):
        date_string = "10-2023"
        expected_date = datetime.datetime(2023, 10, 1)
        self.assertEqual(parse_date(date_string, "%d-%m-%Y"), expected_date)

    def test_get_final_day_of_(self):
        date = datetime.datetime(2023, 2, 15)
        expected_date = datetime.datetime(2023, 2, 28)
        self.assertEqual(get_final_day_of_(date), expected_date)

    def test_next_month_first_day(self):
        date = datetime.datetime(2023, 1, 31)
        expected_date = datetime.datetime(2023, 2, 1)
        self.assertEqual(next_month_first_day(date), expected_date)

    def test_get_yearly_date_range_array_from_(self):
        start_date = datetime.datetime(2021, 6, 1)
        end_date = datetime.datetime(2023, 3, 31)
        expected_ranges = [
            DateRange(datetime.datetime(2021, 6, 1), datetime.datetime(2021, 12, 31)),
            DateRange(datetime.datetime(2022, 1, 1), datetime.datetime(2022, 12, 31)),
            DateRange(datetime.datetime(2023, 1, 1), datetime.datetime(2023, 3, 31)),
        ]
        self.assertEqual(
            get_yearly_date_range_array_from_(start_date, end_date), expected_ranges
        )

    def test_get_monthly_date_range_array_from_(self):
        start_date = datetime.datetime(2023, 1, 15)
        end_date = datetime.datetime(2023, 3, 10)
        expected_ranges = [
            DateRange(datetime.datetime(2023, 1, 15), datetime.datetime(2023, 1, 31)),
            DateRange(datetime.datetime(2023, 2, 1), datetime.datetime(2023, 2, 28)),
            DateRange(datetime.datetime(2023, 3, 1), datetime.datetime(2023, 3, 10)),
        ]
        self.assertEqual(
            get_monthly_date_range_array_from_(start_date, end_date), expected_ranges
        )


if __name__ == "__main__":
    unittest.main()
