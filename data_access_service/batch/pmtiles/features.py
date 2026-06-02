from typing import Dict
from .geometry import build_hex_geometry


def build_hex_feature(
    cell: str,
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

    feature = {
        "type": "Feature",
        "id": cell,
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
