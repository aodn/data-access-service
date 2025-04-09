# tests/utils/test_date_time_utils.py
import unittest
import datetime

from data_access_service.models.date_range import DateRange
from data_access_service.utils.date_time_utils import parse_date, get_final_day_of_, \
    next_month_first_day, get_yearly_date_range_array_from_, get_monthly_date_range_array_from_


class TestDateTimeUtils(unittest.TestCase):

    expected_result = "2023-01-01"

    def test_parse_date_standard_format(self):
        self.assertEqual(parse_date("2023-01-01", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_with_time(self):
        self.assertEqual(
            parse_date("2023-01-01T12:00:00", "%Y-%m-%d"), self.expected_result
        )

    def test_parse_date_different_format(self):
        self.assertEqual(parse_date("01/01/2023", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_with_timezone(self):
        self.assertEqual(
            parse_date("2023-01-01T12:00:00+00:00", "%Y-%m-%d"), self.expected_result
        )

    def test_parse_date_with_text_month(self):
        self.assertEqual(
            parse_date("January 1, 2023", "%Y-%m-%d"), self.expected_result
        )

    def test_parse_date_no_day(self):
        self.assertEqual(parse_date("01-2023", "%Y-%m-%d"), self.expected_result)
        self.assertEqual(parse_date("1-2023", "%Y-%m-%d"), self.expected_result)

    def test_get_final_day(self):
        self.assertEqual(
            get_final_day_of_(datetime.datetime(2023, 1, 1)),
            datetime.datetime(2023, 1, 31),
        )

    def test_next_month_first_daty(self):
        self.assertEqual(
            next_month_first_day(datetime.datetime(2023, 12, 31)),
            datetime.datetime(2024, 1, 1),
        )

    def test_get_yearly_date_range_array_from_(self):
        self.assertEqual(
            get_yearly_date_range_array_from_(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 12, 28)),
            [
                DateRange(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 12, 28)),
            ],
        )

        self.assertEqual(
            get_yearly_date_range_array_from_(datetime.datetime(2022, 1, 2), datetime.datetime(2023, 2, 25)),
            [
                DateRange(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 12, 31)),
                DateRange(datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 25)),
            ],
        )

    def test_get_monthly_date_range_array_from_(self):
        self.assertEqual(
            get_monthly_date_range_array_from_(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 1, 28)),
            [
                DateRange(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 1, 28)),
            ],
        )
        self.assertEqual(
            get_monthly_date_range_array_from_(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 2, 28)),
            [
                DateRange(datetime.datetime(2022, 1, 2), datetime.datetime(2022, 1, 31)),
                DateRange(datetime.datetime(2022, 2, 1), datetime.datetime(2022, 2, 28)),
            ],
        )

if __name__ == "__main__":
    unittest.main()
