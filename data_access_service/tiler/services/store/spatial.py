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
    w = max(1, min(max_dim, int(round((lon_max - lon_min) / lon_spacing))))
    h = max(1, min(max_dim, int(round((lat_max - lat_min) / lat_spacing))))
    return w, h


def default_bbox_from_store(
    store: str,
) -> tuple[float, float, float, float]:
    """The store's EPSG:4326 bounds, with lon clamped to 180."""
    meta = get_store_metadata(store)
    lat_min, lat_max = min(meta.lat), max(meta.lat)
    lon_min, lon_max = min(meta.lon), max(meta.lon)
    if lon_min > 180:
        lon_min -= 360
    if lon_max > 180:
        lon_max = 180.0
    return (lon_min, lat_min, lon_max, lat_max)
