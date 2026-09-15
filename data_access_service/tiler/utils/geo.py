"""Tile and bbox helpers in WGS84 / Web Mercator."""

from __future__ import annotations

import math

from pyproj import Transformer

_mercator_to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


def xyz_tile_wgs84_bbox(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """Geographic bounds of a Web Mercator XYZ tile: lon_min, lat_min, lon_max, lat_max."""
    n = 2**z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0

    def _lat(ty: float) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * ty / n))))

    lat_max = _lat(y)
    lat_min = _lat(y + 1)
    return lon_min, lat_min, lon_max, lat_max


def bbox_to_wgs84(
    bbox: tuple[float, float, float, float], crs: str
) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    if crs.upper() == "EPSG:3857":
        lon_min, lat_min = _mercator_to_wgs84.transform(minx, miny)
        lon_max, lat_max = _mercator_to_wgs84.transform(maxx, maxy)
        return lon_min, lat_min, lon_max, lat_max
    return minx, miny, maxx, maxy


def latlon_to_ij(
    lat: float,
    lon: float,
    *,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    n_i: int,
    n_j: int,
) -> tuple[int, int]:
    """Map a lat/lon to cell indices. Row 0 is north (lat_max)."""
    if n_i <= 1:
        i = 0
    else:
        frac = (lat_max - lat) / (lat_max - lat_min) if lat_max != lat_min else 0.0
        i = int(round(frac * (n_i - 1)))
    if n_j <= 1:
        j = 0
    else:
        frac = (lon - lon_min) / (lon_max - lon_min) if lon_max != lon_min else 0.0
        j = int(round(frac * (n_j - 1)))
    return max(0, min(n_i - 1, i)), max(0, min(n_j - 1, j))


def bbox_to_ij_window(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    *,
    grid_lat_min: float,
    grid_lat_max: float,
    grid_lon_min: float,
    grid_lon_max: float,
    n_i: int,
    n_j: int,
) -> tuple[int, int, int, int] | None:
    """Inclusive i/j window covering a WGS84 bbox, or None if no overlap."""
    if lon_max < grid_lon_min or lon_min > grid_lon_max:
        return None
    if lat_max < grid_lat_min or lat_min > grid_lat_max:
        return None
    i0, j0 = latlon_to_ij(
        lat_max,
        lon_min,
        lat_min=grid_lat_min,
        lat_max=grid_lat_max,
        lon_min=grid_lon_min,
        lon_max=grid_lon_max,
        n_i=n_i,
        n_j=n_j,
    )
    i1, j1 = latlon_to_ij(
        lat_min,
        lon_max,
        lat_min=grid_lat_min,
        lat_max=grid_lat_max,
        lon_min=grid_lon_min,
        lon_max=grid_lon_max,
        n_i=n_i,
        n_j=n_j,
    )
    if i0 > i1:
        i0, i1 = i1, i0
    if j0 > j1:
        j0, j1 = j1, j0
    return i0, i1, j0, j1
