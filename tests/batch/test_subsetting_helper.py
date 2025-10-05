import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from data_access_service.batch.subsetting_helper import trim_date_range_for_keys
from data_access_service.utils.date_time_utils import supply_day_with_nano_precision


class TestTrimDateRangeForKeys(unittest.TestCase):
    @patch("data_access_service.batch.subsetting_helper.api_setup")
    def test_trim_date_range_for_keys(self, mock_api_setup):
        # Mock API and its get_temporal_extent method
        mock_api = MagicMock()
        mock_api.get_temporal_extent.side_effect = [
            (
                pd.Timestamp("2020-01-01 00:00:00.000000000"),
                pd.Timestamp("2020-12-31 23:59:59.999999999"),
            ),
            (
                pd.Timestamp("2020-06-01 00:00:00.000000000"),
                pd.Timestamp("2021-06-30 23:59:59.999999999"),
            ),
        ]
        mock_api_setup.return_value = mock_api

        uuid = "test-uuid"
        keys = ["key1", "key2"]

        # pretent user didn't specify start and end date
        requested_start = "1970-01-01"
        requested_end = pd.Timestamp.now().strftime("%Y-%m-%d")

        requested_start, requested_end = supply_day_with_nano_precision(
            requested_start,
            requested_end,
        )

        trimmed_start, trimmed_end = trim_date_range_for_keys(
            api=mock_api,
            uuid=uuid,
            keys=keys,
            requested_start_date=requested_start,
            requested_end_date=requested_end,
        )

        # The min start is 2020-01-01, max end is 2021-06-30
        self.assertEqual(
            trimmed_start,
            pd.Timestamp(
                year=2020,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                nanosecond=0,
                tz="UTC",
            ),
        )
        self.assertEqual(
            trimmed_end,
            pd.Timestamp(
                year=2021,
                month=6,
                day=30,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
                nanosecond=999,
                tz="UTC",
            ),
        )
