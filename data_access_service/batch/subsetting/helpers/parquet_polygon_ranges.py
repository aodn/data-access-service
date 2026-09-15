"""Polygon-range preparation for the PARQUET batch download.

A dataset without a time column cannot be split into date windows, but it can
be split on its hive `polygon` partition. The partition values are sorted and
cut into half-open ranges `[lo, hi)` the same way `split_date_range` cuts dates:
the first range has no lower bound, the last no upper bound, and neighbours
share a boundary. Every value falls into exactly one range, so child jobs can
neither duplicate nor drop a partition, even if the partitions change between
init and a child.
"""

import logging
import re
from typing import Optional

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

from data_access_service.core.api import BaseAPI

log = logging.getLogger(__name__)

POLYGON_PARTITION = "polygon"

# [lo, hi) - None means unbounded on that side
PolygonRange = list[Optional[str]]


def uses_polygon_sharding(api: BaseAPI, uuid: str, keys: list[str]) -> bool:
    """True when every key has no time column and is partitioned by polygon.

    Anything else keeps the date-range workflow unchanged.
    """
    if not keys:
        return False
    for key in keys:
        # Partition keys first: an unknown key has none, and resolving its
        # column names would raise on the missing metadata
        if POLYGON_PARTITION not in api.get_partition_keys(uuid, key):
            return False
        if api.resolve_dim_names(uuid, key)[2] is not None:
            return False
    return True


_POLYGON_IN_PATH = re.compile(rf".*/{POLYGON_PARTITION}=([^/]*)/")


def list_polygon_values(datasource: ParquetDataSource) -> list[str]:
    """Sorted `polygon` partition values. Reads directory names, not files.

    Not query_unique_value: it caches by id(dataset) and never evicts, so a new
    dataset that reuses a collected one's id gets that dataset's polygons.
    """
    return sorted(
        {
            match.group(1)
            for fragment in datasource.dataset.get_fragments()
            if (match := _POLYGON_IN_PATH.match(fragment.path))
        }
    )


def _shortest_boundary(previous: str, current: str) -> str:
    """Shortest prefix of `current` that still sorts after `previous`.

    Any string in (previous, current] separates the two values; a prefix of the
    hex WKB is far shorter than the full value, which keeps the Batch parameter
    small.
    """
    for length in range(1, len(current) + 1):
        prefix = current[:length]
        if prefix > previous:
            return prefix
    return current


def split_polygon_ranges(
    values: list[str], polygon_count_per_job: int
) -> dict[str, PolygonRange]:
    """Group sorted polygon values into the ranges one Batch array job hands its
    children.

    :param values: polygon partition values, need not be sorted or unique
    :param polygon_count_per_job: how many polygons one child job covers
    :return: {child job index: [lo, hi)}, the first lo and the last hi are None

    Example:
        split_polygon_ranges(["aa", "ab", "ba", "bb"], polygon_count_per_job=2)
        -> {"0": [None, "b"], "1": ["b", None]}
    """
    if polygon_count_per_job <= 0:
        raise ValueError("polygon_count_per_job must be greater than zero")

    ordered = sorted(set(values))
    boundaries: list[Optional[str]] = [None]
    for start in range(polygon_count_per_job, len(ordered), polygon_count_per_job):
        boundaries.append(_shortest_boundary(ordered[start - 1], ordered[start]))
    boundaries.append(None)

    return {
        str(index): [boundaries[index], boundaries[index + 1]]
        for index in range(len(boundaries) - 1)
    }


def select_polygons_in_range(
    values: list[str], polygon_range: PolygonRange
) -> list[str]:
    """Sorted values that fall in `[lo, hi)`."""
    lo, hi = polygon_range
    return sorted(
        value
        for value in set(values)
        if (lo is None or lo <= value) and (hi is None or value < hi)
    )
