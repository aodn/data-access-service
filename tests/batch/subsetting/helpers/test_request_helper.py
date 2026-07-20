import unittest
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_access_service.batch.subsetting.helpers.request_helper import (
    get_subset_request,
)
from data_access_service.utils.subsetting_resolver import (
    trim_date_range_for_keys,
)
from data_access_service.utils.date_time_utils import supply_day_with_nano_precision

_GLOBAL_POLYGON = (
    '{"type":"MultiPolygon","coordinates":'
    "[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}"
)

_VALID_PARAMETERS = {
    "uuid": "test-uuid",
    "key": "file_a.zarr",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "recipient": "test@example.com",
    "multi_polygon": _GLOBAL_POLYGON,
    "output_format": "netcdf",
}


class TestGetSubsetRequest:
    """Input parsing: how get_subset_request turns a parameter dict into a SubsetRequest."""

    def test_comma_separated_key_is_split(self):
        req = get_subset_request({**_VALID_PARAMETERS, "key": "a.zarr,b.zarr,c.zarr"})
        assert req.keys == ["a.zarr", "b.zarr", "c.zarr"]

    def test_whitespace_around_keys_is_stripped(self):
        req = get_subset_request(
            {**_VALID_PARAMETERS, "key": "a.zarr, b.zarr , c.zarr"}
        )
        assert req.keys == ["a.zarr", "b.zarr", "c.zarr"]

    def test_missing_key_defaults_to_wildcard(self):
        params = {**_VALID_PARAMETERS}
        del params["key"]
        req = get_subset_request(params)
        assert req.keys == ["*"]

    def test_multi_polygon_is_parsed_into_bboxes(self):
        req = get_subset_request(_VALID_PARAMETERS)
        assert len(req.bboxes) == 1
        bbox = req.bboxes[0]
        assert (bbox.min_lat, bbox.max_lat) == (-90, 90)
        assert (bbox.min_lon, bbox.max_lon) == (-180, 180)

    def test_non_specified_multi_polygon_yields_global_bbox(self):
        req = get_subset_request(
            {**_VALID_PARAMETERS, "multi_polygon": "non-specified"}
        )
        assert len(req.bboxes) == 1
        bbox = req.bboxes[0]
        assert (bbox.min_lat, bbox.max_lat) == (-90, 90)
        assert (bbox.min_lon, bbox.max_lon) == (-180, 180)

    def test_unset_email_metadata_defaults_to_none(self):
        req = get_subset_request(_VALID_PARAMETERS)
        assert req.collection_title is None
        assert req.full_metadata_link is None
        assert req.suggested_citation is None

    def test_explicit_output_format_is_respected(self):
        req = get_subset_request({**_VALID_PARAMETERS, "output_format": "geotiff"})
        assert req.output_format == "geotiff"

    def test_missing_output_format_raises(self):
        params = {**_VALID_PARAMETERS}
        del params["output_format"]
        with pytest.raises(ValueError, match="output_format"):
            get_subset_request(params)


class TestTrimDateRangeForKeys(unittest.TestCase):
    def test_trim_date_range_for_keys(self):
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

        uuid = "test-uuid"
        keys = ["key1", "key2"]

        # pretend user didn't specify start and end date
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

    def test_trim_date_range_for_keys_range_outside(self):
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

        uuid = "test-uuid"
        keys = ["key1", "key2"]

        # pretend user didn't specify start and end date
        requested_start = "1970-01-01"
        requested_end = "1971-01-01"

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

        self.assertIsNone(trimmed_start)
        self.assertIsNone(trimmed_end)
