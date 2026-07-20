"""Unit tests for utils.subsetting_resolver - the single owner of
"apply the user's subset" behaviour shared by the batch subsetting paths."""

import pandas as pd
import pytest
from unittest.mock import MagicMock

from data_access_service.core.constants import WHOLE_GLOBE_BBOX
from data_access_service.utils.subsetting_resolver import (
    resolve_bboxes,
    resolve_date_range,
    resolve_subset,
)

UUID = "test-uuid"

_SINGLE_POLYGON = (
    '{"type":"MultiPolygon","coordinates":'
    "[[[[10,20],[30,20],[30,40],[10,40],[10,20]]]]}"
)


def _mock_api(known_keys=("a.zarr", "b.zarr"), extent=(None, None)):
    """An api stub exposing the three methods the resolver uses."""
    api = MagicMock()
    api.get_mapped_meta_data.return_value = {k: object() for k in known_keys}
    api.get_temporal_extent.return_value = extent
    return api


class TestResolveSubset:
    def test_explicit_keys_kept_even_if_unknown(self):
        api = _mock_api(known_keys=["a.zarr"])

        # a key that doesn't exist raises KeyError from get_temporal_extent
        # (as API.get_temporal_extent does); the trim must survive it.
        def extent_or_key_error(uuid, key):
            if key == "a.zarr":
                return (None, None)
            raise KeyError(key)

        api.get_temporal_extent.side_effect = extent_or_key_error
        resolved = resolve_subset(
            api, UUID, ["a.zarr", "ghost.zarr"], "2024-01-01", "2024-12-31", None
        )
        # unknown keys stay in the list (callers report/skip them downstream)
        assert resolved.keys == ["a.zarr", "ghost.zarr"]
        assert resolved.has_data

    def test_star_expands_to_all_keys(self):
        api = _mock_api(known_keys=["a.zarr", "b.zarr"])
        resolved = resolve_subset(api, UUID, ["*"], "2024-01-01", "2024-12-31", None)
        assert resolved.keys == ["a.zarr", "b.zarr"]
        api.get_mapped_meta_data.assert_called_once_with(UUID)

    def test_absent_keys_means_all_keys(self):
        api = _mock_api(known_keys=["only.zarr"])
        resolved = resolve_subset(api, UUID, None, "2024-01-01", "2024-12-31", None)
        assert resolved.keys == ["only.zarr"]

    def test_dates_trimmed_to_extent(self):
        # nanosecond-precision extents, as the real get_temporal_extent returns
        api = _mock_api(
            known_keys=["a.zarr"],
            extent=(
                pd.Timestamp("2020-06-01 00:00:00.000000000"),
                pd.Timestamp("2020-12-31 23:59:59.999999999"),
            ),
        )
        resolved = resolve_subset(
            api, UUID, ["a.zarr"], "2020-01-01", "2021-12-31", None
        )
        assert resolved.has_data
        assert resolved.start_date == pd.Timestamp("2020-06-01", tz="UTC")
        assert resolved.end_date == pd.Timestamp(
            "2020-12-31 23:59:59.999999999", tz="UTC"
        )

    def test_range_outside_extent_has_no_data(self):
        api = _mock_api(
            known_keys=["a.zarr"],
            extent=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-12-31")),
        )
        resolved = resolve_subset(
            api, UUID, ["a.zarr"], "1990-01-01", "1990-12-31", None
        )
        assert not resolved.has_data
        assert resolved.start_date is None and resolved.end_date is None

    def test_unknown_extent_keeps_requested_dates(self):
        # get_temporal_extent returning (None, None) (e.g. in tests) must not trim.
        api = _mock_api(known_keys=["a.zarr"], extent=(None, None))
        resolved = resolve_subset(
            api, UUID, ["a.zarr"], "2024-01-01", "2024-12-31", None
        )
        expected_start, expected_end = resolve_date_range("2024-01-01", "2024-12-31")
        assert resolved.start_date == expected_start
        assert resolved.end_date == expected_end

    def test_start_after_end_raises(self):
        api = _mock_api()
        with pytest.raises(ValueError):
            resolve_subset(api, UUID, ["a.zarr"], "2024-12-31", "2024-01-01", None)

    def test_unparseable_date_raises(self):
        api = _mock_api()
        with pytest.raises((ValueError, TypeError)):
            resolve_subset(api, UUID, ["a.zarr"], "not-a-date", "2024-01-01", None)


class TestResolveDateRange:
    def test_non_specified_dates_resolve_to_defaults(self):
        start, end = resolve_date_range("non-specified", "non-specified")
        assert start == pd.Timestamp("1970-01-01", tz="UTC")
        assert end.date() == pd.Timestamp.today().date()

    def test_month_format_supplies_days(self):
        start, end = resolve_date_range("02-2024", "02-2024")
        assert start == pd.Timestamp("2024-02-01", tz="UTC")
        assert end == pd.Timestamp("2024-02-29 23:59:59.999999999", tz="UTC")


class TestResolveBboxes:
    def test_none_means_no_spatial_filter(self):
        assert resolve_bboxes(None) == []

    def test_non_specified_means_no_spatial_filter(self):
        assert resolve_bboxes("non-specified") == []

    def test_geojson_string(self):
        bboxes = resolve_bboxes(_SINGLE_POLYGON)
        assert len(bboxes) == 1
        assert (bboxes[0].min_lon, bboxes[0].min_lat) == (10, 20)
        assert (bboxes[0].max_lon, bboxes[0].max_lat) == (30, 40)

    def test_geojson_dict(self):
        as_dict = {
            "type": "MultiPolygon",
            "coordinates": [[[[10, 20], [30, 20], [30, 40], [10, 40], [10, 20]]]],
        }
        bboxes = resolve_bboxes(as_dict)
        assert len(bboxes) == 1
        assert (bboxes[0].min_lon, bboxes[0].max_lon) == (10, 30)

    def test_non_multipolygon_geometry_raises(self):
        with pytest.raises(TypeError):
            resolve_bboxes('{"type": "Point", "coordinates": [10, 20]}')


def test_whole_globe_bbox_bounds():
    assert (WHOLE_GLOBE_BBOX.min_lon, WHOLE_GLOBE_BBOX.max_lon) == (-180, 180)
    assert (WHOLE_GLOBE_BBOX.min_lat, WHOLE_GLOBE_BBOX.max_lat) == (-90, 90)
