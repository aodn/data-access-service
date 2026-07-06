from typing import Dict
from .geometry_helper import build_hex_geometry


def build_hex_feature(
    cell: str | None,
    month_counts: Dict[int, int],
    layer_name: str,
    minzoom: int,
    maxzoom: int,
    include_tippecanoe_metadata: bool,
) -> Dict:
    properties: Dict = {"h": cell}

    for ym in sorted(month_counts):
        count = int(month_counts[ym])
        if count != 0:
            properties[f"m{ym}"] = count

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
