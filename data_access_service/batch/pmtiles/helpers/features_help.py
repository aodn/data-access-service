from typing import Dict

from data_access_service.models.pmtiles_types import (
    PERIOD_PROPERTY_PREFIX,
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
    apply_period_counts(properties, period_counts, grain)

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
