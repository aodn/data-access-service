import h3

from data_access_service.batch.pmtiles.helpers.geometry_helper import (
    build_hex_geometry,
)

# Arctic res-2 cell whose unwrapped ring self-intersects at ~280.68 E, 87.82 N.
# GEOS used to raise TopologyException: side location conflict here.
_POLAR_ANTIMERIDIAN_CELL = "820327fffffffff"


def _lons(coordinates) -> list[float]:
    if not coordinates:
        return []
    first = coordinates[0]
    # Polygon: [ring]; MultiPolygon: [[ring], ...]
    if first and isinstance(first[0][0], (int, float)):
        rings = coordinates
    else:
        rings = [ring for poly in coordinates for ring in poly]
    return [lon for ring in rings for lon, _lat in ring]


def test_non_crossing_cell_is_polygon_in_world_bounds():
    cell = h3.latlng_to_cell(-33.0, 151.0, 5)
    geo = build_hex_geometry(cell)
    assert geo["type"] == "Polygon"
    lons = _lons(geo["coordinates"])
    assert lons
    assert all(-180 <= lon <= 180 for lon in lons)


def test_equatorial_antimeridian_cell_stays_inside_world_bounds():
    cell = h3.latlng_to_cell(10.0, 179.5, 3)
    geo = build_hex_geometry(cell)
    lons = _lons(geo["coordinates"])
    assert lons
    assert all(-180 <= lon <= 180 for lon in lons)
    assert geo["type"] in {"Polygon", "MultiPolygon"}


def test_polar_antimeridian_cell_does_not_raise_topology_exception():
    geo = build_hex_geometry(_POLAR_ANTIMERIDIAN_CELL)
    lons = _lons(geo["coordinates"])
    assert lons
    assert all(-180 <= lon <= 180 for lon in lons)
    assert geo["type"] in {"Polygon", "MultiPolygon"}
    if geo["type"] == "MultiPolygon":
        assert len(geo["coordinates"]) >= 2
