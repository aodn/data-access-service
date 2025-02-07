# tests/utils/test_date_time_utils.py
import unittest
from data_access_service.utils.date_time_utils import parse_date

class TestDateTimeUtils(unittest.TestCase):

    expected_result = "2023-01-01"

    def test_parse_date_standard_format(self):
        self.assertEqual(parse_date("2023-01-01", "%Y-%m-%d"),self.expected_result)

    def test_parse_date_with_time(self):
        self.assertEqual(parse_date("2023-01-01T12:00:00", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_different_format(self):
        self.assertEqual(parse_date("01/01/2023", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_with_timezone(self):
        self.assertEqual(parse_date("2023-01-01T12:00:00+00:00", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_with_text_month(self):
        self.assertEqual(parse_date("January 1, 2023", "%Y-%m-%d"), self.expected_result)

    def test_parse_date_no_day(self):
        self.assertEqual( parse_date("01-2023", "%Y-%m-%d"),self.expected_result )
        self.assertEqual(parse_date("1-2023", "%Y-%m-%d"), self.expected_result)

if __name__ == "__main__":
    unittest.main()