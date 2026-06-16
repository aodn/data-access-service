from typing import List, Dict
import h3
from shapely.geometry import Polygon, MultiPolygon, mapping
from shapely.ops import unary_union

_WORLD_BOUNDS = Polygon([(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)])


def h3_boundary_lnglat(cell: str) -> List[List[float]]:
    try:
        valid = bool(cell) and h3.is_valid_cell(cell)
    except (OverflowError, ValueError):
        valid = False
    if not valid:
        raise ValueError(f"Invalid H3 cell: {cell!r}")

    # Resolve boundary function from available h3 API surface
    boundary_getter = getattr(h3, "cell_to_boundary", None) or getattr(
        h3, "h3_to_geo_boundary", None
    )
    if boundary_getter is None:
        raise RuntimeError(
            "h3 library does not expose a boundary function (expected 'cell_to_boundary' or 'h3_to_geo_boundary')"
        )

    boundary = boundary_getter(cell)

    ring = [[float(lng), float(lat)] for lat, lng in boundary]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def _crosses_antimeridian(ring: List[List[float]]) -> bool:
    for i in range(len(ring) - 1):
        if abs(ring[i][0] - ring[i + 1][0]) > 180:
            return True
    return False


def _unwrap_ring(ring: List[List[float]]) -> List[List[float]]:
    unwrapped = [ring[0][:]]
    for i in range(1, len(ring)):
        prev_lng = unwrapped[-1][0]
        curr_lng = ring[i][0]
        delta = curr_lng - prev_lng
        if delta > 180:
            curr_lng -= 360
        elif delta < -180:
            curr_lng += 360
        unwrapped.append([curr_lng, ring[i][1]])
    return unwrapped


def build_hex_geometry(cell: str) -> Dict:
    ring = h3_boundary_lnglat(cell)

    if not _crosses_antimeridian(ring):
        return {"type": "Polygon", "coordinates": [ring]}

    unwrapped = _unwrap_ring(ring)
    poly = Polygon(unwrapped)

    pieces = []
    for shift in (0, -360, 360):
        shifted = Polygon([(x + shift, y) for x, y in poly.exterior.coords])
        clipped = shifted.intersection(_WORLD_BOUNDS)
        if not clipped.is_empty and clipped.area > 0:
            pieces.append(clipped)

    if not pieces:
        return {"type": "Polygon", "coordinates": [ring]}

    merged = unary_union(pieces)
    if isinstance(merged, Polygon):
        merged = MultiPolygon([merged])

    geo = mapping(merged)
    return {"type": geo["type"], "coordinates": geo["coordinates"]}
