"""Spatial helpers based on a store's grid: CRS conversion, native
resolution, default bounds."""

from pyproj import Transformer

from data_access_service.tiler.services.store.registry import get_store_metadata

_mercator_to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


def bbox_to_wgs84(
    bbox: tuple[float, float, float, float], crs: str
) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    if crs == "EPSG:3857":
        lon_min, lat_min = _mercator_to_wgs84.transform(minx, miny)
        lon_max, lat_max = _mercator_to_wgs84.transform(maxx, maxy)
        return lon_min, lat_min, lon_max, lat_max
    return minx, miny, maxx, maxy


def native_resolution_in_bbox(
    store: str,
    bbox_wgs84: tuple[float, float, float, float],
    max_dim: int = 2048,
) -> tuple[int, int]:
    """(width, height) at the grid's native resolution inside the bbox,
    clamped to ``[1, max_dim]``. Assumes a regular grid."""
    meta = get_store_metadata(store)
    lat_spacing = abs(meta.lat[1] - meta.lat[0])
    lon_spacing = abs(meta.lon[1] - meta.lon[0])
    lon_min, lat_min, lon_max, lat_max = bbox_wgs84
    w = max(1, min(max_dim, int(round(lon_span(lon_min, lon_max) / lon_spacing))))
    h = max(1, min(max_dim, int(round((lat_max - lat_min) / lat_spacing))))
    return w, h


def wrap_lon(lon: float) -> float:
    """A grid lon (up to 360) as -180..180. 180 stays 180."""
    return lon - 360.0 if lon > 180.0 else lon


def lon_span(west: float, east: float) -> float:
    """Degrees from west to east; a bbox crossing 180° has west > east."""
    span = east - west
    return span + 360.0 if span < 0 else span


def default_bbox_from_store(
    store: str,
) -> tuple[float, float, float, float]:
    """The store's EPSG:4326 bounds, lon in -180..180. A grid crossing 180°
    gets west > east."""
    meta = get_store_metadata(store)
    lat_min, lat_max = min(meta.lat), max(meta.lat)
    lon_min, lon_max = float(min(meta.lon)), float(max(meta.lon))
    res = abs(float(meta.lon[1] - meta.lon[0])) if len(meta.lon) > 1 else 0.0
    if lon_max - lon_min + res >= 360.0:
        return (-180.0, lat_min, 180.0, lat_max)
    return (wrap_lon(lon_min), lat_min, wrap_lon(lon_max), lat_max)
