from typing import Dict

from data_access_service.models.pmtiles_types import (
    PERIOD_PROPERTY_PREFIX,
    SINGLE_TIME_GROUP_BY,
    TimeGroupBy,
)

from .geometry_helper import build_hex_geometry


def period_property_key(period: int, grain: TimeGroupBy) -> str:
    """Feature property name for a count bucket, e.g. ``d20240115`` or ``m202401``."""
    prefix = PERIOD_PROPERTY_PREFIX.get(grain)
    if prefix is None:
        raise ValueError(f"No property prefix for grain {grain!r}")
    return f"{prefix}{int(period)}"


def apply_period_counts(
    properties: Dict,
    period_counts: Dict[int, int],
    grain: TimeGroupBy,
) -> None:
    """Write non-zero period counts onto a feature properties dict."""
    for period in sorted(period_counts):
        count = int(period_counts[period])
        if count != 0:
            properties[period_property_key(period, grain)] = count


def rollup_day_counts(
    day_counts: Dict[int, int],
) -> Dict[TimeGroupBy, Dict[int, int]]:
    """Roll day-level (YYYYMMDD) counts into month and year buckets.

    Returns maps for DATE (pass-through), MONTH (YYYYMM), and YEAR (YYYY).
    """
    month_counts: Dict[int, int] = {}
    year_counts: Dict[int, int] = {}
    for period, count in day_counts.items():
        c = int(count)
        if c == 0:
            continue
        day = int(period)
        month_counts[day // 100] = month_counts.get(day // 100, 0) + c
        year_counts[day // 10000] = year_counts.get(day // 10000, 0) + c
    return {
        TimeGroupBy.DATE: {
            int(p): int(c) for p, c in day_counts.items() if int(c) != 0
        },
        TimeGroupBy.MONTH: month_counts,
        TimeGroupBy.YEAR: year_counts,
    }


def apply_all_period_counts(
    properties: Dict,
    day_counts: Dict[int, int],
) -> None:
    """Write day, month, and year count properties derived from day-level counts."""
    for grain, counts in rollup_day_counts(day_counts).items():
        apply_period_counts(properties, counts, grain)


def build_hex_feature(
    cell: str | None,
    period_counts: Dict[int, int],
    layer_name: str,
    minzoom: int,
    maxzoom: int,
    include_tippecanoe_metadata: bool,
    grain: TimeGroupBy = TimeGroupBy.MONTH,
) -> Dict:
    properties: Dict = {"h": cell}
    if grain == TimeGroupBy.ALL:
        # period_counts are day keys (YYYYMMDD); roll up month and year.
        apply_all_period_counts(properties, period_counts)
    elif grain in SINGLE_TIME_GROUP_BY:
        apply_period_counts(properties, period_counts, grain)
    else:
        raise ValueError(f"Unsupported time grain for feature properties: {grain!r}")

    # No feature "id": Tippecanoe only accepts numeric IDs and H3 cell values
    # exceed the safe integer range anyway; the cell is in properties["h"].
    feature = {
        "type": "Feature",
        "properties": properties,
        "geometry": build_hex_geometry(cell),
    }

    if include_tippecanoe_metadata:
        feature["tippecanoe"] = {
            "layer": layer_name,
            "minzoom": int(minzoom),
            "maxzoom": int(maxzoom),
        }

    return feature
