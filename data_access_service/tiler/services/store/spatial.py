"""Store-aware spatial helpers: CRS conversion, native cell resolution, default bounds.

Sits between the store registry and the visual-tile pipeline. Without it,
routers and ``rendering.visual_tiles`` would reach into the registry directly
(``store.lat.values``, ``store.lon.values``) to compute things like bbox
resolution; keeping that data-access logic here lets the HTTP layer talk in
domain terms (``native_resolution_in_bbox(product, bbox)``) instead of xarray
internals.
"""

import numpy as np
from pyproj import Transformer
from rio_tiler.constants import WEB_MERCATOR_TMS

from data_access_service.tiler.services.store.registry import get_store

_mercator_to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

# (lon_min, lat_min, lon_max, lat_max) in the store's own lon frame.
ReadBBox = tuple[float, float, float, float]


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
    product_source_path: str,
    bbox_wgs84: tuple[float, float, float, float],
    max_dim: int = 2048,
) -> tuple[int, int]:
    """Output dimensions that match the dataset's native cell resolution inside the bbox.

    Clamped to ``[1, max_dim]`` per axis so a huge bbox over a high-resolution grid
    can't blow the response up to an unreasonable size. Cell spacing is read from
    the first two lat/lon coordinates — all current products are on regular grids;
    irregular grids would need a different code path.
    """
    store = get_store(product_source_path)
    lat_vals = store.lat.values
    lon_vals = store.lon.values
    lat_spacing = abs(float(lat_vals[1] - lat_vals[0]))
    lon_spacing = abs(float(lon_vals[1] - lon_vals[0]))
    lon_min, lat_min, lon_max, lat_max = bbox_wgs84
    w = max(1, min(max_dim, int(round((lon_max - lon_min) / lon_spacing))))
    h = max(1, min(max_dim, int(round((lat_max - lat_min) / lat_spacing))))
    return w, h


def default_bbox_from_store(
    product_source_path: str,
) -> tuple[float, float, float, float]:
    """Return EPSG:4326 bounds for the dataset, clamped to ±180 lon.

    Antimeridian-straddling datasets (e.g. GSLA at 57–185°E) lose the sliver past
    180° in the default rendering — callers can pass an explicit bbox to cover
    the other side.
    """
    store = get_store(product_source_path)
    lat_min = float(store.lat.min())
    lat_max = float(store.lat.max())
    lon_min = float(store.lon.min())
    lon_max = float(store.lon.max())
    if lon_min > 180:
        lon_min -= 360
    if lon_max > 180:
        lon_max = 180.0
    return (lon_min, lat_min, lon_max, lat_max)


def xyz_tile_wgs84_bbox(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """Geographic bounds of a Web Mercator XYZ tile, as (lon_min, lat_min, lon_max, lat_max)."""
    bounds = WEB_MERCATOR_TMS.bounds(x, y, z)
    return (
        float(bounds.left),
        float(bounds.bottom),
        float(bounds.right),
        float(bounds.top),
    )


def _spatial_chunk_sizes(store) -> tuple[int, int] | None:
    """Return (lat_chunk, lon_chunk) from Zarr encoding, or None if unknown."""
    for da in store.data_vars.values():
        chunks = da.encoding.get("chunks")
        if not chunks:
            continue
        try:
            lat_i = da.dims.index("lat")
            lon_i = da.dims.index("lon")
        except ValueError:
            continue
        return int(chunks[lat_i]), int(chunks[lon_i])
    return None


def _snap_1d(
    values: np.ndarray, lo: float, hi: float, chunk: int | None
) -> tuple[float, float] | None:
    """Expand [lo, hi] to covering chunk edges on a 1-D coord. None if no overlap."""
    if values.size == 0:
        return None
    descending = bool(values[0] > values[-1])
    ordered = values[::-1] if descending else values
    # searchsorted on ascending copy
    i0 = int(np.searchsorted(ordered, lo, side="left"))
    i1 = int(np.searchsorted(ordered, hi, side="right"))
    if i1 <= i0:
        # lo/hi may sit between cells or entirely outside; include nearest
        # in-range cell if the window touches the grid at all.
        if hi < ordered[0] or lo > ordered[-1]:
            return None
        i0 = max(0, min(i0, ordered.size - 1))
        i1 = i0 + 1
    if chunk and chunk > 0:
        i0 = (i0 // chunk) * chunk
        i1 = min(ordered.size, ((i1 + chunk - 1) // chunk) * chunk)
        i1 = max(i1, i0 + 1)
    i0 = max(0, i0)
    i1 = min(ordered.size, max(i1, i0 + 1))
    snapped_lo = float(ordered[i0])
    snapped_hi = float(ordered[i1 - 1])
    return (snapped_lo, snapped_hi)


def snap_read_bbox(
    store_url: str,
    bbox_wgs84: tuple[float, float, float, float],
    pad_cells: int = 2,
) -> ReadBBox | None:
    """Intersect ``bbox_wgs84`` with the store, pad, and snap to Zarr chunks.

    Returns ``(lon_min, lat_min, lon_max, lat_max)`` in the store's own longitude
    frame (so 0–360 stores stay 0–360), ready to pass to ``ZarrDataSource.get_data``.
    ``None`` means the window does not overlap the store — the caller should not
    fetch data.

    Pad is in native cells (bilinear halo + optional coastal-fill distance).
    Snapping to chunk edges means neighbouring tiles that touch the same on-disk
    chunks share one L1 key instead of each paying their own decompress.
    """
    store = get_store(store_url)
    lat = np.asarray(store.lat.values)
    lon = np.asarray(store.lon.values)
    if lat.size < 1 or lon.size < 1:
        return None

    s_lat0, s_lat1 = float(np.min(lat)), float(np.max(lat))
    s_lon0, s_lon1 = float(np.min(lon)), float(np.max(lon))

    lat_spacing = abs(float(lat[1] - lat[0])) if lat.size > 1 else 0.0
    lon_spacing = abs(float(lon[1] - lon[0])) if lon.size > 1 else 0.0
    pad_lat = pad_cells * lat_spacing
    pad_lon = pad_cells * lon_spacing

    r_lon0, r_lat0, r_lon1, r_lat1 = bbox_wgs84
    r_lon0 -= pad_lon
    r_lon1 += pad_lon
    r_lat0 -= pad_lat
    r_lat1 += pad_lat

    lat_lo = max(r_lat0, s_lat0)
    lat_hi = min(r_lat1, s_lat1)
    if lat_hi < lat_lo:
        return None

    # Request is WGS84 (−180..180). Stores may be 0..360 (e.g. GSLA 57–185).
    # Try the request, and ±360, and keep the largest overlap with the store.
    best: tuple[float, float] | None = None
    best_span = -1.0
    for shift in (0.0, 360.0, -360.0):
        a, b = r_lon0 + shift, r_lon1 + shift
        lo = max(a, s_lon0)
        hi = min(b, s_lon1)
        if hi < lo:
            continue
        span = hi - lo
        if span > best_span:
            best_span = span
            best = (lo, hi)
    if best is None:
        return None
    lon_lo, lon_hi = best

    chunks = _spatial_chunk_sizes(store)
    lat_chunk = chunks[0] if chunks else None
    lon_chunk = chunks[1] if chunks else None

    lat_snapped = _snap_1d(lat, lat_lo, lat_hi, lat_chunk)
    lon_snapped = _snap_1d(lon, lon_lo, lon_hi, lon_chunk)
    if lat_snapped is None or lon_snapped is None:
        return None
    return (lon_snapped[0], lat_snapped[0], lon_snapped[1], lat_snapped[1])


def round_bbox(bbox: ReadBBox) -> ReadBBox:
    """Stable cache-key form of a read window (≈0.1 m at the equator)."""
    return tuple(round(v, 6) for v in bbox)  # type: ignore[return-value]
