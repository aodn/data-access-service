import json
from typing import Any, Dict

from data_access_service.models.pmtiles_types import (
    COUNTS_PROPERTY,
    DAYS_KEY,
    SINGLE_TIME_GROUP_BY,
    TOTAL_KEY,
    TimeGroupBy,
)

from .geometry_helper import build_hex_geometry


def build_counts_tree_from_days(day_counts: Dict[int, int]) -> Dict[str, Any]:
    """Build year→month→d→day tree with ``t`` totals from YYYYMMDD keys."""
    years: Dict[str, Any] = {}
    for period, count in day_counts.items():
        c = int(count)
        if c == 0:
            continue
        day = int(period)
        y = f"{day // 10000:04d}"
        m = f"{(day // 100) % 100:02d}"
        d = f"{day % 100:02d}"
        year_node = years.setdefault(y, {})
        month_node = year_node.setdefault(m, {DAYS_KEY: {}})
        days = month_node[DAYS_KEY]
        days[d] = days.get(d, 0) + c

    for year_node in years.values():
        year_total = 0
        for key, month_node in year_node.items():
            if key == TOTAL_KEY:
                continue
            days = month_node.get(DAYS_KEY, {})
            month_total = int(sum(int(v) for v in days.values()))
            month_node[TOTAL_KEY] = month_total
            year_total += month_total
        year_node[TOTAL_KEY] = year_total
    return years


def build_counts_tree_from_months(month_counts: Dict[int, int]) -> Dict[str, Any]:
    """Build year→month tree with ``t`` only (no day map) from YYYYMM keys."""
    years: Dict[str, Any] = {}
    for period, count in month_counts.items():
        c = int(count)
        if c == 0:
            continue
        ym = int(period)
        y = f"{ym // 100:04d}"
        m = f"{ym % 100:02d}"
        year_node = years.setdefault(y, {})
        year_node[m] = {TOTAL_KEY: c}

    for year_node in years.values():
        year_node[TOTAL_KEY] = int(
            sum(
                int(node[TOTAL_KEY])
                for key, node in year_node.items()
                if key != TOTAL_KEY
            )
        )
    return years


def build_counts_tree_from_years(year_counts: Dict[int, int]) -> Dict[str, Any]:
    """Build year→``t`` tree from YYYY keys."""
    return {
        f"{int(period):04d}": {TOTAL_KEY: int(count)}
        for period, count in year_counts.items()
        if int(count) != 0
    }


def counts_tree_for_grain(
    period_counts: Dict[int, int], grain: TimeGroupBy
) -> Dict[str, Any]:
    """Build the nested counts tree for the configured time grain."""
    if grain in (TimeGroupBy.DATE, TimeGroupBy.ALL):
        return build_counts_tree_from_days(period_counts)
    if grain == TimeGroupBy.MONTH:
        return build_counts_tree_from_months(period_counts)
    if grain == TimeGroupBy.YEAR:
        return build_counts_tree_from_years(period_counts)
    raise ValueError(f"Unsupported time grain for counts tree: {grain!r}")


def apply_counts_tree(properties: Dict, tree: Dict[str, Any]) -> None:
    """Serialize nested counts tree onto feature properties as JSON string ``c``."""
    properties[COUNTS_PROPERTY] = json.dumps(tree, separators=(",", ":"))


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
    if grain not in SINGLE_TIME_GROUP_BY and grain != TimeGroupBy.ALL:
        raise ValueError(f"Unsupported time grain for feature properties: {grain!r}")
    apply_counts_tree(properties, counts_tree_for_grain(period_counts, grain))

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
