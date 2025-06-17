# tests/utils/test_date_time_utils.py
import unittest
import pandas as pd

from datetime import datetime
from unittest.mock import MagicMock

import pytz

from data_access_service.core.api import BaseAPI
from data_access_service.utils.date_time_utils import (
    parse_date,
    get_final_day_of_month_,
    next_month_first_day,
    trim_date_range,
    get_monthly_date_range_array_from_,
    get_boundary_of_year_month,
    transfer_date_range_into_yearmonth,
    split_yearmonths_into_dict,
    ensure_timezone,
)


class TestDateTimeUtils(unittest.TestCase):
    def setUp(self):
        self.api = BaseAPI()

    def test_parse_date(self):
        date_string = "2023-10-01"
        expected_date = datetime(2023, 10, 1)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y-%m-%d"), expected_date
        )

    def test_parse_date2(self):
        date_string = "2023/10/01"
        expected_date = datetime(2023, 10, 1)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y/%m/%d"), expected_date
        )

    def test_parse_date3(self):
        date_string = "2023.10.01"
        expected_date = datetime(2023, 10, 1)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y.%m.%d"), expected_date
        )

    def test_parse_date4(self):
        date_string = "10-2023"
        expected_date = datetime(2023, 10, 1)

        with self.assertRaises(ValueError) as cm:
            self.assertEqual(
                parse_date(date_string, format_to_convert="%d-%m-%Y"), expected_date
            )
            self.assertEqual(
                str(cm.exception),
                "time data '10-2023' does not match format '%d-%m-%Y'",
            )

    def test_get_final_day_of_(self):
        date = datetime(2023, 2, 15)
        expected_date = datetime(2023, 2, 28)
        self.assertEqual(get_final_day_of_month_(date), expected_date)

    def test_next_month_first_day(self):
        date = datetime(2023, 1, 31)
        expected_date = datetime(2023, 2, 1)
        self.assertEqual(next_month_first_day(date), expected_date)

    def test_get_monthly_date_range_array_from_(self):
        """Test a typical date range spanning multiple months with partial months."""
        start = datetime(2023, 1, 15)
        end = datetime(2023, 4, 10)
        expected = [
            {
                "start_date": datetime(2023, 1, 15),
                "end_date": datetime(2023, 1, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 2, 1),
                "end_date": datetime(2023, 2, 28, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 3, 1),
                "end_date": datetime(2023, 3, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 4, 1),
                "end_date": datetime(2023, 4, 10, 23, 59, 59),
            },
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Monthly ranges do not match expected output"
        )

    def test_single_month(self):
        """Test a date range within a single month."""
        start = datetime(2023, 2, 1)
        end = datetime(2023, 2, 15)
        expected = [
            {
                "start_date": datetime(2023, 2, 1),
                "end_date": datetime(2023, 2, 15, 23, 59, 59),
            }
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Single month range does not match expected output"
        )

    def test_single_day(self):
        """Test a date range of a single day."""
        start = datetime(2023, 3, 5)
        end = datetime(2023, 3, 5)
        expected = [
            {
                "start_date": datetime(2023, 3, 5),
                "end_date": datetime(2023, 3, 5, 23, 59, 59),
            }
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Single day range does not match expected output"
        )

    def test_full_year(self):
        """Test a date range spanning a full year."""
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)
        expected = [
            {
                "start_date": datetime(2023, 1, 1),
                "end_date": datetime(2023, 1, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 2, 1),
                "end_date": datetime(2023, 2, 28, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 3, 1),
                "end_date": datetime(2023, 3, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 4, 1),
                "end_date": datetime(2023, 4, 30, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 5, 1),
                "end_date": datetime(2023, 5, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 6, 1),
                "end_date": datetime(2023, 6, 30, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 7, 1),
                "end_date": datetime(2023, 7, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 8, 1),
                "end_date": datetime(2023, 8, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 9, 1),
                "end_date": datetime(2023, 9, 30, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 10, 1),
                "end_date": datetime(2023, 10, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 11, 1),
                "end_date": datetime(2023, 11, 30, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 12, 1),
                "end_date": datetime(2023, 12, 31, 23, 59, 59),
            },
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Full year range does not match expected output"
        )

    def test_leap_year(self):
        """Test a date range including February in a leap year."""
        start = datetime(2024, 2, 1)
        end = datetime(2024, 3, 31)
        expected = [
            {
                "start_date": datetime(2024, 2, 1),
                "end_date": datetime(2024, 2, 29, 23, 59, 59),
            },
            {
                "start_date": datetime(2024, 3, 1),
                "end_date": datetime(2024, 3, 31, 23, 59, 59),
            },
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Leap year range does not match expected output"
        )

    def test_cross_year(self):
        """Test a date range spanning multiple years."""
        start = datetime(2022, 12, 15)
        end = datetime(2023, 1, 15)
        expected = [
            {
                "start_date": datetime(2022, 12, 15),
                "end_date": datetime(2022, 12, 31, 23, 59, 59),
            },
            {
                "start_date": datetime(2023, 1, 1),
                "end_date": datetime(2023, 1, 15, 23, 59, 59),
            },
        ]
        result = get_monthly_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Cross-year range does not match expected output"
        )

    def test_invalid_date_range(self):
        """Test when end_date is before start_date."""
        start = datetime(2023, 1, 15)
        end = datetime(2023, 1, 14)
        with self.assertRaises(
            ValueError, msg="Expected ValueError for end_date before start_date"
        ):
            get_monthly_date_range_array_from_(start, end)

    def test_output_type(self):
        """Test that start_date and end_date are datetime objects."""
        start = datetime(2023, 1, 15)
        end = datetime(2023, 2, 15)
        result = get_monthly_date_range_array_from_(start, end)
        for item in result:
            self.assertIsInstance(
                item["start_date"], type(start), "start_date is not datetime"
            )
            self.assertIsInstance(
                item["end_date"], type(end), "end_date is not datetime"
            )
            self.assertNotIsInstance(
                item["start_date"],
                pd.Timestamp,
                "start_date should not be pandas.Timestamp",
            )
            self.assertNotIsInstance(
                item["end_date"],
                pd.Timestamp,
                "end_date should not be pandas.Timestamp",
            )
            self.assertTrue(
                "start_date" in item and "end_date" in item, "Dictionary keys missing"
            )

    def test_fully_within_metadata_range(self):
        """Test when requested range is fully within metadata range."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 6, 1)
        requested_end = datetime(2023, 6, 30)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual((requested_start, requested_end), result)
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_partial_overlap_start_before(self):
        """Test when requested start is before metadata start."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2022, 12, 1)
        requested_end = datetime(2023, 6, 30)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (metadata_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_partial_overlap_end_after(self):
        """Test when requested end is after metadata end."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 6, 1)
        requested_end = datetime(2024, 1, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, metadata_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_partial_overlap_both_outside(self):
        """Test when requested range spans metadata range."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2022, 12, 1)
        requested_end = datetime(2024, 1, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (metadata_start, metadata_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_exactly_matches_metadata_range(self):
        """Test when requested range exactly matches metadata range."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_no_overlap_before_metadata(self):
        """Test when requested range is entirely before metadata range."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2022, 1, 1)
        requested_end = datetime(2022, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (None, None))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_no_overlap_after_metadata(self):
        """Test when requested range is entirely after metadata range."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2024, 1, 1)
        requested_end = datetime(2024, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (None, None))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_same_requested_dates_within_metadata(self):
        """Test when requested start and end dates are the same (within metadata)."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 6, 1)
        requested_end = datetime(2023, 6, 1)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_requested_matches_metadata_start(self):
        """Test when requested range starts at metadata start."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 6, 30)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_same_metadata_dates(self):
        """Test when metadata start and end dates are the same."""
        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 1, 1)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 1, 1)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_invalid_metadata_empty(self):
        """Test when metadata temporal extent is empty. The original start & end dates should be returned."""
        self.api.get_temporal_extent = MagicMock(return_value=())

        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_invalid_metadata_single_element(self):
        """Test when metadata temporal extent has one element. just return the original start & end dates."""
        self.api.get_temporal_extent = MagicMock(return_value=(datetime(2023, 1, 1),))

        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_invalid_metadata_too_many_elements(self):
        """Test when metadata temporal extent has more than two elements. just return the original start & end dates."""
        self.api.get_temporal_extent = MagicMock(
            return_value=(
                datetime(2023, 1, 1),
                datetime(2023, 12, 31),
                datetime(2024, 1, 1),
            )
        )

        requested_start = datetime(2023, 1, 1)
        requested_end = datetime(2023, 12, 31)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_timezone_stripped_metadata(self):
        """Test when metadata dates have timezone info (should be stripped)."""
        from datetime import timezone

        metadata_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        metadata_end = datetime(2023, 12, 31, tzinfo=timezone.utc)
        metadata_start_naive = metadata_start.replace(tzinfo=None)
        metadata_end_naive = metadata_end.replace(tzinfo=None)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = datetime(2023, 6, 1)
        requested_end = datetime(2023, 6, 30)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        self.assertEqual(result, (requested_start, requested_end))
        self.assertIsNone(result[0].tzinfo)
        self.assertIsNone(result[1].tzinfo)
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_timezone_stripped_requested(self):
        """Test when requested dates have timezone info (should be preserved in logic but naive in output)."""
        from datetime import timezone

        metadata_start = datetime(2023, 1, 1)
        metadata_end = datetime(2023, 12, 31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )

        requested_start = datetime(2023, 6, 1, tzinfo=timezone.utc)
        requested_end = datetime(2023, 6, 30, tzinfo=timezone.utc)

        result = trim_date_range(self.api, "test-uuid", requested_start, requested_end)

        requested_start_naive = requested_start.replace(tzinfo=None)
        requested_end_naive = requested_end.replace(tzinfo=None)

        self.assertEqual(result, (requested_start_naive, requested_end_naive))
        self.assertIsNone(result[0].tzinfo)
        self.assertIsNone(result[1].tzinfo)
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_api_failure(self):
        """Test when API call fails."""
        self.api.get_temporal_extent = MagicMock(side_effect=Exception("API error"))

        with self.assertRaises(Exception) as cm:
            trim_date_range(
                self.api, "test-uuid", datetime(2023, 1, 1), datetime(2023, 12, 31)
            )

        self.assertEqual(str(cm.exception), "API error")
        self.api.get_temporal_extent.assert_called_with(uuid="test-uuid")

    def test_get_boundary_of_year_month(self):
        """Test the boundary of a year-month string."""
        year_month_str = "2023-10"
        expected_start = datetime(2023, 10, 1, 0, 0, 0)
        expected_end = datetime(2023, 10, 31, 23, 59, 59)

        start_date, end_date = get_boundary_of_year_month(year_month_str)

        self.assertEqual(start_date, expected_start)
        self.assertEqual(end_date, expected_end)

        year_month_str2 = "02-2023"
        expected_start2 = datetime(2023, 2, 1, 0, 0, 0)
        expected_end2 = datetime(2023, 2, 28, 23, 59, 59)

        start_date2, end_date2 = get_boundary_of_year_month(year_month_str2)
        self.assertEqual(start_date2, expected_start2)
        self.assertEqual(end_date2, expected_end2)

    def test_transfer_date_range_into_yearmonth(self):
        start_date = "09-2020"
        end_date = "10-2021"

        yearmonths = transfer_date_range_into_yearmonth(
            start_date=start_date, end_date=end_date
        )
        expected_yearmonths = [
            "09-2020",
            "10-2020",
            "11-2020",
            "12-2020",
            "01-2021",
            "02-2021",
            "03-2021",
            "04-2021",
            "05-2021",
            "06-2021",
            "07-2021",
            "08-2021",
            "09-2021",
            "10-2021",
        ]
        self.assertEqual(yearmonths, expected_yearmonths)

    def test_split_yearmonths_into_dict(self):
        yearmonths = [
            "2023-01",
            "2023-02",
            "2023-03",
            "2023-04",
            "2023-05",
            "2023-06",
            "2023-07",
            "2023-08",
            "2023-09",
            "2023-10",
        ]
        expected_dict = {
            0: ["2023-01", "2023-02", "2023-03"],
            1: ["2023-04", "2023-05", "2023-06"],
            2: ["2023-07", "2023-08", "2023-09"],
            3: ["2023-10"],
        }
        result = split_yearmonths_into_dict(yearmonths, 3)
        self.assertEqual(result, expected_dict)

    def test_datetime_without_timezone(self):
        # Test case: datetime without timezone should be assigned UTC
        dt = datetime(2025, 6, 12, 8, 34)
        result = ensure_timezone(dt)
        self.assertEqual(result.tzinfo, pytz.UTC)
        self.assertEqual(result, datetime(2025, 6, 12, 8, 34, tzinfo=pytz.UTC))

    def test_datetime_with_timezone(self):
        # Test case: datetime with timezone should remain unchanged
        tz = pytz.timezone("US/Pacific")
        dt = datetime(2025, 6, 12, 8, 34, tzinfo=tz)
        result = ensure_timezone(dt)
        self.assertEqual(result.tzinfo, tz)
        self.assertEqual(result, dt)

    def test_datetime_with_different_timezone(self):
        # Test case: datetime with non-UTC timezone should remain unchanged
        tz = pytz.timezone("Asia/Tokyo")
        dt = datetime(2025, 6, 12, 8, 34, tzinfo=tz)
        result = ensure_timezone(dt)
        self.assertEqual(result.tzinfo, tz)
        self.assertEqual(result, dt)

    def test_datetime_preservation(self):
        # Test case: ensure original datetime components are preserved
        dt = datetime(2025, 6, 12, 8, 34, 56, 123456)
        result = ensure_timezone(dt)
        self.assertEqual(result.year, 2025)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 12)
        self.assertEqual(result.hour, 8)
        self.assertEqual(result.minute, 34)
        self.assertEqual(result.second, 56)
        self.assertEqual(result.microsecond, 123456)
        self.assertEqual(result.tzinfo, pytz.UTC)
