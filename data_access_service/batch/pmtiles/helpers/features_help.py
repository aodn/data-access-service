from typing import Dict
from .geometry_helper import build_hex_geometry


def build_hex_feature(
    cell: str | None,
    date_counts: Dict[int, int],
    layer_name: str,
    minzoom: int,
    maxzoom: int,
    include_tippecanoe_metadata: bool,
) -> Dict:
    properties: Dict = {"h": cell}

    for date_key in sorted(date_counts):
        count = int(date_counts[date_key])
        if count != 0:
            properties[f"m{date_key}"] = count

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
