# tests/utils/test_date_time_utils.py
import unittest
import pandas as pd

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch
from datetime import timezone

import pytz

from data_access_service.core.api import BaseAPI
from data_access_service.utils.date_time_utils import (
    parse_date,
    get_final_day_of_month_,
    next_month_first_day,
    trim_date_range,
    get_monthly_utc_date_range_array_from_,
    get_boundary_of_year_month,
    transfer_date_range_into_yearmonth,
    split_yearmonths_into_dict,
    ensure_timezone,
    split_date_range,
    split_date_range_binary,
    check_rows_with_date_range,
)


class TestDateTimeUtils(unittest.TestCase):
    def setUp(self):
        self.api = BaseAPI()

    def test_parse_date(self):
        date_string = "2023-10-01"
        expected_date = pd.Timestamp(year=2023, month=10, day=1, tz=pytz.UTC)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y-%m-%d"), expected_date
        )

    def test_parse_date2(self):
        date_string = "2023/10/01"
        expected_date = pd.Timestamp(year=2023, month=10, day=1, tz=pytz.UTC)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y/%m/%d"), expected_date
        )

    def test_parse_date3(self):
        date_string = "2023.10.01"
        expected_date = pd.Timestamp(2023, 10, 1, tz=pytz.UTC)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y.%m.%d"), expected_date
        )

    def test_parse_date4(self):
        date_string = "2010-04-30 23:59:59.999999999"
        expected_date = pd.Timestamp(
            year=2010,
            month=4,
            day=30,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
            nanosecond=999,
            tz=pytz.UTC,
        )

        self.assertEqual(parse_date(date_string), expected_date)

    def test_parse_date5(self):
        date_string = "2023-10-01"
        expected_date = pd.Timestamp(year=2023, month=10, day=1, tz=pytz.UTC)
        self.assertEqual(
            parse_date(date_string, format_to_convert="%Y-%m-%d"), expected_date
        )

    def test_get_final_day_of_(self):
        date = pd.Timestamp(year=2023, month=2, day=15)
        expected_date = pd.Timestamp(
            year=2023,
            month=2,
            day=28,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
            nanosecond=999,
            tz=pytz.UTC,
        )
        target_date = get_final_day_of_month_(date)
        self.assertEqual(target_date, expected_date)

    def test_next_month_first_day(self):
        date = pd.Timestamp(year=2023, month=1, day=31)
        expected_date = pd.Timestamp(year=2023, month=2, day=1, tz=pytz.UTC)
        self.assertEqual(next_month_first_day(date), expected_date)

        date = pd.Timestamp(year=2023, month=1, day=31, tz="Asia/Tokyo")
        expected_date = pd.Timestamp(year=2023, month=2, day=1, tz="Asia/Tokyo")
        self.assertEqual(next_month_first_day(date), expected_date)

    def test_get_monthly_date_range_array_from_(self):
        """
        Test a typical date range spanning multiple months with partial months. With edge case fall on nanoseconds
        """
        start = pd.Timestamp(
            year=2023,
            month=1,
            day=15,
            hour=5,
            minute=53,
            second=1,
            microsecond=80043,
            nanosecond=8,
            tz="Asia/Tokyo",
        )
        end = pd.Timestamp(year=2023, month=4, day=10, nanosecond=1, tz="Asia/Tokyo")
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=14,
                    hour=20,
                    minute=53,
                    second=1,
                    microsecond=80043,
                    nanosecond=8,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=2,
                    day=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=2,
                    day=28,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=3,
                    day=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=3,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=4,
                    day=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=4,
                    day=9,
                    hour=15,
                    minute=0,
                    second=0,
                    microsecond=0,
                    nanosecond=1,
                    tz=pytz.UTC,
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Monthly ranges do not match expected output"
        )

    def test_get_monthly_date_range_array_from_same_start_end(self):
        """
        Test a date range where start and end are the same. the precision of timestamp is nanoseconds.
        """
        start = pd.Timestamp(
            year=2023,
            month=4,
            day=9,
            hour=15,
            minute=0,
            second=0,
            microsecond=0,
            nanosecond=1,
            tz=pytz.UTC,
        )
        end = pd.Timestamp(
            year=2023,
            month=4,
            day=9,
            hour=15,
            minute=0,
            second=0,
            microsecond=0,
            nanosecond=1,
            tz=pytz.UTC,
        )
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=4,
                    day=9,
                    hour=15,
                    minute=0,
                    second=0,
                    microsecond=0,
                    nanosecond=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=4,
                    day=9,
                    hour=15,
                    minute=0,
                    second=0,
                    microsecond=0,
                    nanosecond=1,
                    tz=pytz.UTC,
                ),
            }
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Monthly ranges do not match expected output"
        )

    def test_get_monthly_date_range_array_from_2_(self):
        """
        Test an edge case where two day are very close to month end
        """
        start = pd.Timestamp(
            year=2023,
            month=1,
            day=31,
            hour=5,
            minute=53,
            second=1,
            microsecond=80043,
            nanosecond=8,
            tz=pytz.UTC,
        )
        end = pd.Timestamp(year=2023, month=2, day=1, nanosecond=1, tz=pytz.UTC)
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=31,
                    hour=5,
                    minute=53,
                    second=1,
                    microsecond=80043,
                    nanosecond=8,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023, month=2, day=1, nanosecond=1, tz=pytz.UTC
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Monthly ranges do not match expected output"
        )

    def test_get_monthly_date_range_array_from_3_(self):
        """
        Test an edge case where two day are very close to month end
        """
        start = pd.Timestamp(
            year=2023,
            month=1,
            day=30,
            hour=5,
            minute=53,
            second=1,
            microsecond=80043,
            nanosecond=8,
            tz=pytz.UTC,
        )
        end = pd.Timestamp(year=2023, month=2, day=1, nanosecond=1, tz=pytz.UTC)
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=30,
                    hour=5,
                    minute=53,
                    second=1,
                    microsecond=80043,
                    nanosecond=8,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=2,
                    day=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023, month=2, day=1, nanosecond=1, tz=pytz.UTC
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Monthly ranges do not match expected output"
        )

    def test_single_month(self):
        """Test a date range within a single month."""
        start = pd.Timestamp(year=2023, month=2, day=1, tz="Australia/Sydney")
        end = pd.Timestamp(year=2023, month=2, day=15, tz="Australia/Sydney")
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023, month=1, day=31, hour=13, tz=pytz.UTC
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=2,
                    day=14,
                    hour=13,
                    tz=pytz.UTC,
                ),
            }
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Single month range does not match expected output"
        )

    def test_single_day(self):
        """Test a date range of a single day."""
        start = pd.Timestamp(year=2023, month=3, day=5, tz="Australia/Sydney")
        end = pd.Timestamp(year=2023, month=3, day=5, hour=10, tz="Australia/Sydney")
        expected = [
            {
                "start_date": pd.Timestamp(
                    year=2023, month=3, day=4, hour=13, tz=pytz.UTC
                ),
                "end_date": pd.Timestamp(
                    year=2023, month=3, day=5, hour=10, tz="Australia/Sydney"
                ),
            }
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Single day range does not match expected output"
        )

    def test_full_year(self):
        """Test a date range spanning a full year."""
        start = pd.Timestamp(year=2023, month=1, day=1)
        end = pd.Timestamp(year=2023, month=12, day=31)
        expected = [
            {
                "start_date": pd.Timestamp(year=2023, month=1, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=2, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=2,
                    day=28,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=3, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=3,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=4, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=4,
                    day=30,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=5, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=5,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=6, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=6,
                    day=30,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=7, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=7,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=8, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=8,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=9, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=9,
                    day=30,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=10, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=10,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=11, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=11,
                    day=30,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2023, month=12, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=12,
                    day=31,
                    tz=pytz.UTC,
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Full year range does not match expected output"
        )

    def test_leap_year(self):
        """Test a date range including February in a leap year."""
        start = pd.Timestamp(year=2024, month=2, day=1)
        end = pd.Timestamp(year=2024, month=3, day=31)
        expected = [
            {
                "start_date": pd.Timestamp(year=2024, month=2, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2024,
                    month=2,
                    day=29,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(year=2024, month=3, day=1, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2024,
                    month=3,
                    day=31,
                    tz=pytz.UTC,
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Leap year range does not match expected output"
        )

    def test_cross_year(self):
        """Test a date range spanning multiple years."""
        start = pd.Timestamp(year=2022, month=12, day=15, tz=pytz.UTC)
        end = pd.Timestamp(year=2023, month=1, day=15, tz=pytz.UTC)
        expected = [
            {
                "start_date": pd.Timestamp(year=2022, month=12, day=15, tz=pytz.UTC),
                "end_date": pd.Timestamp(
                    year=2022,
                    month=12,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                    nanosecond=999,
                    tz=pytz.UTC,
                ),
            },
            {
                "start_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=1,
                    tz=pytz.UTC,
                ),
                "end_date": pd.Timestamp(
                    year=2023,
                    month=1,
                    day=15,
                    tz=pytz.UTC,
                ),
            },
        ]
        result = get_monthly_utc_date_range_array_from_(start, end)
        self.assertListEqual(
            result, expected, "Cross-year range does not match expected output"
        )

    def test_invalid_date_range(self):
        """Test when end_date is before start_date."""
        start = pd.Timestamp(year=2023, month=1, day=15)
        end = pd.Timestamp(year=2023, month=1, day=14)
        with self.assertRaises(
            ValueError, msg="Expected ValueError for end_date before start_date"
        ):
            get_monthly_utc_date_range_array_from_(start, end)

    def test_output_type(self):
        """Test that start_date and end_date are datetime objects."""
        start = pd.Timestamp(year=2023, month=1, day=15)
        end = pd.Timestamp(year=2023, month=2, day=15)
        result = get_monthly_utc_date_range_array_from_(start, end)
        for item in result:
            self.assertIsInstance(
                item["start_date"], type(start), "start_date is not datetime"
            )
            self.assertIsInstance(
                item["end_date"], type(end), "end_date is not datetime"
            )
            self.assertTrue(
                "start_date" in item and "end_date" in item, "Dictionary keys missing"
            )

    def test_fully_within_metadata_range(self):
        """Test when requested range is fully within metadata range."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=6, day=1)
        requested_end = pd.Timestamp(year=2023, month=6, day=30)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual((requested_start, requested_end), result)
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_partial_overlap_start_before(self):
        """Test when requested start is before metadata start."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2022, month=12, day=1)
        requested_end = pd.Timestamp(year=2023, month=6, day=30)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (metadata_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_partial_overlap_end_after(self):
        """Test when requested end is after metadata end."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=6, day=1)
        requested_end = pd.Timestamp(year=2024, month=1, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, metadata_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_partial_overlap_both_outside(self):
        """Test when requested range spans metadata range."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2022, month=12, day=1)
        requested_end = pd.Timestamp(year=2024, month=1, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (metadata_start, metadata_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_exactly_matches_metadata_range(self):
        """Test when requested range exactly matches metadata range."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_no_overlap_before_metadata(self):
        """Test when requested range is entirely before metadata range."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2022, month=1, day=1)
        requested_end = pd.Timestamp(year=2022, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (None, None))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_no_overlap_after_metadata(self):
        """Test when requested range is entirely after metadata range."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2024, month=1, day=1)
        requested_end = pd.Timestamp(year=2024, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (None, None))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_same_requested_dates_within_metadata(self):
        """Test when requested start and end dates are the same (within metadata)."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=6, day=1)
        requested_end = pd.Timestamp(year=2023, month=6, day=1)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_requested_matches_metadata_start(self):
        """Test when requested range starts at metadata start."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=6, day=30)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_same_metadata_dates(self):
        """Test when metadata start and end dates are the same."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=1, day=1)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=1, day=1)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_invalid_metadata_empty(self):
        """Test when metadata temporal extent is empty. The original start & end dates should be returned."""
        self.api.get_temporal_extent = MagicMock(return_value=())

        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_invalid_metadata_single_element(self):
        """Test when metadata temporal extent has one element. just return the original start & end dates."""
        self.api.get_temporal_extent = MagicMock(return_value=(datetime(2023, 1, 1),))

        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_invalid_metadata_too_many_elements(self):
        """Test when metadata temporal extent has more than two elements. just return the original start & end dates."""
        self.api.get_temporal_extent = MagicMock(
            return_value=(
                pd.Timestamp(year=2023, month=1, day=1),
                pd.Timestamp(year=2023, month=12, day=31),
                pd.Timestamp(year=2024, month=1, day=1),
            )
        )

        requested_start = pd.Timestamp(year=2023, month=1, day=1)
        requested_end = pd.Timestamp(year=2023, month=12, day=31)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_timezone_stripped_metadata(self):
        """Test when metadata dates have timezone info (should be stripped)."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1, tz=pytz.UTC)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31, tz=pytz.UTC)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )
        requested_start = pd.Timestamp(year=2023, month=6, day=1)
        requested_end = pd.Timestamp(year=2023, month=6, day=30)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        self.assertEqual(result, (requested_start, requested_end))
        self.assertIsNone(result[0].tzinfo)
        self.assertIsNone(result[1].tzinfo)
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_timezone_stripped_requested(self):
        """Test when requested dates have timezone info (should be preserved in logic but naive in output)."""
        metadata_start = pd.Timestamp(year=2023, month=1, day=1)
        metadata_end = pd.Timestamp(year=2023, month=12, day=31)
        self.api.get_temporal_extent = MagicMock(
            return_value=(metadata_start, metadata_end)
        )

        requested_start = pd.Timestamp(year=2023, month=6, day=1, tz=pytz.UTC)
        requested_end = pd.Timestamp(year=2023, month=6, day=30, tz=pytz.UTC)

        result = trim_date_range(
            self.api, "test-uuid", "test-key", requested_start, requested_end
        )

        requested_start_naive = requested_start.replace(tzinfo=None)
        requested_end_naive = requested_end.replace(tzinfo=None)

        self.assertEqual(result, (requested_start_naive, requested_end_naive))
        self.assertIsNone(result[0].tz)
        self.assertIsNone(result[1].tz)
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_api_failure(self):
        """Test when API call fails."""
        self.api.get_temporal_extent = MagicMock(side_effect=Exception("API error"))

        with self.assertRaises(Exception) as cm:
            trim_date_range(
                self.api,
                "test-uuid",
                "test-key",
                datetime(2023, 1, 1),
                datetime(2023, 12, 31),
            )

        self.assertEqual(str(cm.exception), "API error")
        self.api.get_temporal_extent.assert_called_with(
            uuid="test-uuid", key="test-key"
        )

    def test_get_boundary_of_year_month(self):
        """Test the boundary of a year-month string."""
        year_month_str = "2023-10"
        expected_start = pd.Timestamp(
            year=2023, month=10, day=1, hour=0, minute=0, second=0, tz=pytz.UTC
        )
        expected_end = pd.Timestamp(
            year=2023,
            month=10,
            day=31,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
            nanosecond=999,
            tz=pytz.UTC,
        )

        start_date, end_date = get_boundary_of_year_month(year_month_str)

        self.assertEqual(start_date, expected_start)
        self.assertEqual(end_date, expected_end)

        year_month_str2 = "02-2023"
        expected_start2 = pd.Timestamp(
            year=2023, month=2, day=1, hour=0, minute=0, second=0, tz=pytz.UTC
        )
        expected_end2 = pd.Timestamp(
            year=2023,
            month=2,
            day=28,
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
            nanosecond=999,
            tz=pytz.UTC,
        )

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
        dt = pd.Timestamp(year=2025, month=6, day=12, hour=8, minute=34)
        result = ensure_timezone(dt)
        self.assertEqual(result.tzinfo, pytz.UTC)
        self.assertEqual(result, datetime(2025, 6, 12, 8, 34, tzinfo=pytz.UTC))

    def test_datetime_with_timezone(self):
        # Test case: datetime with timezone should remain unchanged
        dt = pd.Timestamp(
            year=2025, month=6, day=12, hour=8, minute=34, tz="US/Pacific"
        )
        result = ensure_timezone(dt)
        self.assertEqual(result.tzname(), "PDT")
        self.assertEqual(result, dt)

    def test_datetime_with_different_timezone(self):
        # Test case: datetime with non-UTC timezone should remain unchanged
        dt = pd.Timestamp(
            year=2025, month=6, day=12, hour=8, minute=34, tz="Asia/Tokyo"
        )
        result = ensure_timezone(dt)
        self.assertEqual(result.tzname(), "JST")
        self.assertEqual(result, dt)

    def test_datetime_preservation(self):
        # Test case: ensure original datetime components are preserved
        dt = pd.Timestamp(
            year=2025, month=6, day=12, hour=8, minute=34, second=56, microsecond=123456
        )
        result = ensure_timezone(dt)
        self.assertEqual(result.year, 2025)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 12)
        self.assertEqual(result.hour, 8)
        self.assertEqual(result.minute, 34)
        self.assertEqual(result.second, 56)
        self.assertEqual(result.microsecond, 123456)
        self.assertEqual(result.tzinfo, pytz.UTC)

    def test_split_date_range(self):
        date_ranges = split_date_range(
            pd.Timestamp("2010-02-01"), pd.Timestamp("2011-05-02 23:59:59.999999999"), 3
        )
        expected_result = {
            0: [
                "2010-02-01 00:00:00.000000000",
                "2010-04-30 23:59:59.999999999",
            ],
            1: [
                "2010-05-01 00:00:00.000000000",
                "2010-07-31 23:59:59.999999999",
            ],
            2: [
                "2010-08-01 00:00:00.000000000",
                "2010-10-31 23:59:59.999999999",
            ],
            3: [
                "2010-11-01 00:00:00.000000000",
                "2011-01-31 23:59:59.999999999",
            ],
            4: [
                "2011-02-01 00:00:00.000000000",
                "2011-04-30 23:59:59.999999999",
            ],
            5: [
                "2011-05-01 00:00:00.000000000",
                "2011-05-02 23:59:59.999999999",
            ],
        }
        self.assertEqual(len(date_ranges), 6)
        self.assertEqual(expected_result, date_ranges)

    def test_split_date_range_binary(self):
        start = ensure_timezone(pd.Timestamp("2010-01-01"))
        end = ensure_timezone(pd.Timestamp("2010-01-03"))

        split_start, split_mid, split_end = split_date_range_binary(start, end)
        expected_split_start = pd.Timestamp("2010-01-01", tz="UTC")
        expected_split_mid = pd.Timestamp("2010-01-02", tz="UTC")
        expected_split_end = pd.Timestamp("2010-01-03", tz="UTC")
        self.assertEqual(
            (split_start, split_mid, split_end),
            (expected_split_start, expected_split_mid, expected_split_end),
        )

    @patch("data_access_service.utils.date_time_utils.PARQUET_SUBSET_ROW_NUMBER", 1000)
    @patch("data_access_service.utils.date_time_utils.MAX_PARQUET_SPLIT", 10)
    @patch("data_access_service.utils.date_time_utils.create_time_filter")
    @patch("data_access_service.utils.date_time_utils.split_date_range_binary")
    @patch("data_access_service.utils.date_time_utils.log")
    def test_check_rows_with_date_range(self, mock_log, mock_split, mock_create_filter):
        mock_ds = Mock()
        mock_ds.dname = "test_data.parquet"
        mock_ds.dataset = Mock()

        mock_filter = Mock()
        mock_create_filter.return_value = mock_filter
        mock_ds.dataset.count_rows.side_effect = [2000, 400, 400]

        mock_date_ranges = [
            {
                "start_date": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "end_date": datetime(2023, 1, 31, tzinfo=timezone.utc),
            },
            {
                "start_date": datetime(2023, 2, 1, tzinfo=timezone.utc),
                "end_date": datetime(2023, 2, 28, tzinfo=timezone.utc),
            },
        ]

        start_date = mock_date_ranges[0]["start_date"]
        end_date = mock_date_ranges[0]["end_date"]
        mid_date = datetime(2023, 1, 15, tzinfo=timezone.utc)
        mock_split.return_value = (start_date, mid_date, end_date)

        result = check_rows_with_date_range(mock_ds, [mock_date_ranges[0]])

        # Should result in 2 split ranges
        self.assertEqual(len(result), 2)
        mock_split.assert_called_once()
        mock_log.info.assert_called_once()
