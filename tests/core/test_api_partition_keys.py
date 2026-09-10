import unittest

import pytest

from data_access_service.core.api import API
from data_access_service.core.constants import STR_TIME_UPPER_CASE

_UUID = "test-uuid"
_KEY = "some_dataset.parquet"

# A dataset whose only time-ish field is the hive partition key: this is the
# shape that produced a 1970 temporal extent in issue 9144.
_NO_REAL_TIME_COLUMN = {
    "ssm_lat": {"type": "double"},
    "ssm_lon": {"type": "double"},
    "timestamp": {"type": "int64", "units": "1", "long_name": "Partition timestamp"},
    "polygon": {"type": "string", "long_name": "Spatial partition polygon"},
}

# Same shape, but written before the long_name marker existed.
_OLD_METADATA = {
    "ssm_lat": {"type": "double"},
    "timestamp": {"type": "int64"},
    "polygon": {"type": "string"},
}

_REAL_TIME_COLUMN = {
    "TIME": {"type": "timestamp[ns]"},
    "timestamp": {"type": "int64", "long_name": "Partition timestamp"},
}


class TestExtractPartitionKeys(unittest.TestCase):
    def test_detected_by_long_name(self):
        self.assertEqual(
            API._extract_partition_keys(_NO_REAL_TIME_COLUMN),
            frozenset({"timestamp", "polygon"}),
        )

    def test_detected_by_name_when_long_name_is_missing(self):
        self.assertEqual(
            API._extract_partition_keys(_OLD_METADATA),
            frozenset({"timestamp", "polygon"}),
        )

    def test_real_timestamp_column_is_not_a_partition_key(self):
        # The reserved name is only rejected when the type is non-temporal, so a
        # dataset that genuinely stores a datetime in "timestamp" still works.
        data = {"timestamp": {"type": "timestamp[ns]"}}

        self.assertEqual(API._extract_partition_keys(data), frozenset())

    def test_ordinary_columns_are_untouched(self):
        data = {"s_date": {"type": "timestamp[ns]"}, "cnt": {"type": "int64"}}

        self.assertEqual(API._extract_partition_keys(data), frozenset())


class TestMapColumnNamesSkipsPartitionKeys(unittest.TestCase):
    def _api(self, schema: dict) -> API:
        api = API()
        api._schema_keys = {_UUID: {_KEY: frozenset(schema.keys())}}
        api._partition_keys = {_UUID: {_KEY: API._extract_partition_keys(schema)}}
        return api

    def test_time_does_not_fall_back_to_the_partition_key(self):
        api = self._api(_NO_REAL_TIME_COLUMN)

        self.assertEqual(api.map_column_names(_UUID, _KEY, [STR_TIME_UPPER_CASE]), [])

    def test_a_real_time_column_still_maps(self):
        api = self._api(_REAL_TIME_COLUMN)

        self.assertEqual(
            api.map_column_names(_UUID, _KEY, [STR_TIME_UPPER_CASE]), ["TIME"]
        )

    def test_require_time_column_raises_instead_of_index_error(self):
        api = self._api(_NO_REAL_TIME_COLUMN)

        with pytest.raises(ValueError, match="No usable time column"):
            api.require_time_column(_UUID, _KEY)

    def test_require_time_column_returns_the_mapped_name(self):
        api = self._api(_REAL_TIME_COLUMN)

        self.assertEqual(api.require_time_column(_UUID, _KEY), "TIME")
