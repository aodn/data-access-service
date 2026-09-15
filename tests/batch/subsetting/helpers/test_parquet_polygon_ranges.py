import json
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.batch.subsetting.helpers.parquet_polygon_ranges import (
    list_polygon_values,
    select_polygons_in_range,
    split_polygon_ranges,
    uses_polygon_sharding,
)

# Shaped like real hive `polygon` values: a shared WKB header, then coordinates
WKB_HEADER = "010300000001000000050000000000"


def _polygon_values(count: int) -> list[str]:
    return [f"{WKB_HEADER}{index:04X}{'C0' * 78}" for index in range(count)]


def _covering_ranges(values, ranges) -> dict[str, list[str]]:
    return {
        value: [
            index
            for index, polygon_range in ranges.items()
            if select_polygons_in_range([value], polygon_range)
        ]
        for value in values
    }


class TestSplitPolygonRanges:
    def test_93_polygons_at_20_per_job_gives_5_ranges(self):
        values = _polygon_values(93)

        ranges = split_polygon_ranges(values, polygon_count_per_job=20)

        assert list(ranges.keys()) == ["0", "1", "2", "3", "4"]
        assert ranges["0"][0] is None
        assert ranges["4"][1] is None
        sizes = [len(select_polygons_in_range(values, r)) for r in ranges.values()]
        assert sizes == [20, 20, 20, 20, 13]

    def test_every_value_is_in_exactly_one_range(self):
        values = _polygon_values(93)

        ranges = split_polygon_ranges(values, polygon_count_per_job=20)

        for value, owners in _covering_ranges(values, ranges).items():
            assert len(owners) == 1, value

    def test_neighbouring_ranges_share_a_boundary(self):
        ranges = split_polygon_ranges(_polygon_values(93), polygon_count_per_job=20)

        ordered = [ranges[str(index)] for index in range(len(ranges))]
        for left, right in zip(ordered, ordered[1:]):
            assert left[1] == right[0]

    def test_boundaries_are_shorter_than_the_values(self):
        values = _polygon_values(93)

        ranges = split_polygon_ranges(values, polygon_count_per_job=20)

        for lo, _ in list(ranges.values())[1:]:
            assert len(lo) < len(values[0])

    def test_fewer_values_than_one_job_gives_one_unbounded_range(self):
        ranges = split_polygon_ranges(_polygon_values(5), polygon_count_per_job=20)

        assert ranges == {"0": [None, None]}

    def test_unsorted_and_duplicate_values_give_the_same_ranges(self):
        values = _polygon_values(45)
        shuffled = list(reversed(values)) + values[:10]

        assert split_polygon_ranges(shuffled, 20) == split_polygon_ranges(values, 20)

    def test_ranges_survive_a_batch_parameter_round_trip(self):
        ranges = split_polygon_ranges(_polygon_values(93), polygon_count_per_job=20)

        assert json.loads(json.dumps(ranges)) == ranges

    def test_rejects_non_positive_count(self):
        with pytest.raises(ValueError):
            split_polygon_ranges(_polygon_values(3), polygon_count_per_job=0)


class TestSelectPolygonsInRange:
    def test_value_added_after_init_is_still_covered_once(self):
        values = _polygon_values(93)
        ranges = split_polygon_ranges(values, polygon_count_per_job=20)

        # A partition that did not exist when init split the ranges
        new_values = [
            values[19] + "0",
            values[20][:-1] + "1",
            "0" * 10,
            "F" * 200,
        ]

        for value, owners in _covering_ranges(new_values, ranges).items():
            assert len(owners) == 1, value


class TestUsesPolygonSharding:
    @staticmethod
    def _api(time_by_key: dict, partitions_by_key: dict) -> MagicMock:
        api = MagicMock()
        api.resolve_dim_names.side_effect = lambda uuid, key: (
            "LATITUDE",
            "LONGITUDE",
            time_by_key[key],
        )
        api.get_partition_keys.side_effect = lambda uuid, key: partitions_by_key[key]
        return api

    def test_no_time_column_with_polygon_partition(self):
        api = self._api({"a": None}, {"a": frozenset({"polygon"})})

        assert uses_polygon_sharding(api, "uuid", ["a"])

    def test_time_column_keeps_the_date_workflow_even_with_polygon_partition(self):
        # e.g. aggregated_seagrass_nonqc: _temporal_extent plus a polygon partition
        api = self._api(
            {"a": "_temporal_extent"}, {"a": frozenset({"timestamp", "polygon"})}
        )

        assert not uses_polygon_sharding(api, "uuid", ["a"])

    def test_no_time_column_without_polygon_partition(self):
        api = self._api({"a": None}, {"a": frozenset()})

        assert not uses_polygon_sharding(api, "uuid", ["a"])

    def test_mixed_keys_keep_the_date_workflow(self):
        api = self._api(
            {"a": None, "b": "TIME"},
            {"a": frozenset({"polygon"}), "b": frozenset({"polygon"})},
        )

        assert not uses_polygon_sharding(api, "uuid", ["a", "b"])

    def test_no_keys(self):
        assert not uses_polygon_sharding(MagicMock(), "uuid", [])

    def test_unknown_key_skips_column_lookup(self):
        # init with a key missing from the metadata must not resolve its columns
        api = MagicMock()
        api.get_partition_keys.return_value = frozenset()
        api.resolve_dim_names.side_effect = KeyError("not_exist")

        assert not uses_polygon_sharding(api, "uuid", ["not_exist"])


class TestListPolygonValues:
    def test_ignores_stale_query_unique_value_cache(self, tmp_path):
        values = _polygon_values(3)
        pq.write_to_dataset(
            pa.table({"site": ["s0", "s1", "s2"], "polygon": values}),
            root_path=str(tmp_path),
            partition_cols=["polygon"],
        )
        datasource = MagicMock()
        datasource.dataset = pa_ds.dataset(
            str(tmp_path), format="parquet", partitioning="hive"
        )
        # what a collected dataset with the same id would have left behind
        DataQuery._partition_value_cache[(id(datasource.dataset), "polygon")] = {
            "stale"
        }
        try:
            assert list_polygon_values(datasource) == sorted(values)
        finally:
            DataQuery._partition_value_cache.pop(
                (id(datasource.dataset), "polygon"), None
            )
