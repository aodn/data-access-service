import h3
from typing import Dict, List

from shapely import make_valid
from shapely.affinity import translate
from shapely.errors import GEOSException
from shapely.geometry import MultiPolygon, Polygon, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

_WORLD_BOUNDS = Polygon([(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)])

# Full-precision floats are ~18 characters each in JSON
# Hexagon coordinates are now rounded to 6 decimal places,
_COORD_DECIMALS = 6


def _round_coordinates(value):
    if isinstance(value, (list, tuple)):
        return [_round_coordinates(item) for item in value]
    return round(float(value), _COORD_DECIMALS)


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

    ring = [
        [round(float(lng), _COORD_DECIMALS), round(float(lat), _COORD_DECIMALS)]
        for lat, lng in boundary
    ]
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


def _polygons_with_area(geometry: BaseGeometry | None) -> List[Polygon]:
    """Flatten overlay results to polygons that have area (drop lines/points)."""
    if geometry is None or geometry.is_empty:
        return []
    if isinstance(geometry, Polygon):
        return [geometry] if geometry.area > 0 else []
    if hasattr(geometry, "geoms"):
        parts: List[Polygon] = []
        for geom in geometry.geoms:
            parts.extend(_polygons_with_area(geom))
        return parts
    return []


def _split_unwrapped_at_dateline(ring: List[List[float]]) -> List[Polygon]:
    """Clip an antimeridian-unwrapped hex into pieces inside [-180, 180].

    Polar H3 cells can unwrap to a self-intersecting ring (GEOS reports
    ``side location conflict``). ``make_valid`` repairs that before clip.
    """
    valid = make_valid(Polygon(ring))
    pieces: List[Polygon] = []
    for shift in (0, -360, 360):
        clipped = translate(valid, xoff=shift).intersection(_WORLD_BOUNDS)
        pieces.extend(_polygons_with_area(clipped))
    return _polygons_with_area(unary_union(pieces)) if pieces else []


def build_hex_geometry(cell: str) -> Dict:
    ring = h3_boundary_lnglat(cell)

    if not _crosses_antimeridian(ring):
        return {"type": "Polygon", "coordinates": [ring]}

    try:
        parts = _split_unwrapped_at_dateline(_unwrap_ring(ring))
    except GEOSException:
        parts = []

    if not parts:
        return {"type": "Polygon", "coordinates": [ring]}

    merged = parts[0] if len(parts) == 1 else MultiPolygon(parts)
    geo = mapping(merged)
    return {"type": geo["type"], "coordinates": _round_coordinates(geo["coordinates"])}
