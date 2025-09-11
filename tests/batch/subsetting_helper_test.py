import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from data_access_service.batch.subsetting_helper import trim_date_range_for_keys


class TestTrimDateRangeForKeys(unittest.TestCase):
    @patch("data_access_service.batch.subsetting_helper.api_setup")
    def test_trim_date_range_for_keys(self, mock_api_setup):
        # Mock API and its get_temporal_extent method
        mock_api = MagicMock()
        mock_api.get_temporal_extent.side_effect = [
            (pd.Timestamp("2020-01-01"), pd.Timestamp("2020-12-31")),
            (pd.Timestamp("2020-06-01"), pd.Timestamp("2021-06-30")),
        ]
        mock_api_setup.return_value = mock_api

        uuid = "test-uuid"
        keys = ["key1", "key2"]

        # pretent user didn't specify start and end date
        requested_start = pd.Timestamp("1970-01-01")
        requested_end = pd.Timestamp.now()

        trimmed_start, trimmed_end = trim_date_range_for_keys(
            uuid=uuid,
            keys=keys,
            requested_start_date=requested_start,
            requested_end_date=requested_end,
        )

        # The min start is 2020-01-01, max end is 2021-06-30
        self.assertEqual(trimmed_start, pd.Timestamp("2020-01-01"))
        self.assertEqual(trimmed_end, pd.Timestamp("2021-06-30"))
