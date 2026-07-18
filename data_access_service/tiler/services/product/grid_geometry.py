"""Geographic geometry of a data-tile LOD grid: square cells, NW-anchored.

One LOD grid is ``total_w x total_h`` pixels (grid cols/rows x chunk_px). Its
geographic mapping is defined by a single isotropic cell size per LOD:

    cell_size = max(lon_span / total_w, lat_span / total_h)      (degrees)

anchored at the source's north-west data edge. The axis that needs the larger
cell exactly fills its pixels; the other axis leaves partially-filled (masked
invalid) pixels on the east/south edge, as does the ceil-based tile rounding.

Why one cell size instead of stretching each axis independently (the pre-cv2
behaviour): a stretched grid has cellSizeX != cellSizeY, which cannot be
represented by a standard OGC TileMatrixSet 2.0 tile matrix (single
``cellSize``) without misstating the geometry — the custom-TMS spike measured
0.9%-6.9% axis disagreement (up to ~289 km of south-edge error) on real stores.
With one cell size the published TMS is exact.

The renderer (``rendering/kernels.py``), the per-date manifest
(``product/manifest.py``) and coverage discovery all derive their mapping from
this module, so the served pixels, the shader manifest, and the OGC TMS can
never disagree. Changing this mapping changes tile bytes: bump CACHE_VERSION.
"""

from dataclasses import dataclass

import xarray as xr


@dataclass(frozen=True)
class GridGeometry:
    """Mapping between one LOD pixel grid and CRS84 degrees.

    ``west``/``north`` are the grid origin (cell edges, not centres): the data's
    west/north edge, i.e. first coordinate centre minus half a native step.
    ``east``/``south`` are the far grid edges: origin + total px * cell_size, so
    they lie on or beyond the data's far edges.
    """

    total_w: int
    total_h: int
    cell_size: float  # degrees per grid pixel, both axes
    west: float
    north: float
    east: float
    south: float
    dlon: float  # native source step, degrees (positive)
    dlat: float
    # cell_size expressed in source steps per axis — the resample kernels' scale
    # factors. Exactly cell_size/dlon and cell_size/dlat.
    sx_ratio: float
    sy_ratio: float


def grid_geometry(ds: xr.Dataset, total_w: int, total_h: int) -> GridGeometry:
    """Compute the LOD grid geometry for a dataset's lat/lon coordinates.

    Assumes 1-D, regularly spaced coordinates (the tiler's source contract);
    the native step is taken from the coordinate extent, matching the uniform
    treatment the resample kernels apply.
    """
    lon = ds.lon.values
    lat = ds.lat.values
    nlon = int(lon.size)
    nlat = int(lat.size)
    if nlon < 2 or nlat < 2:
        raise ValueError(
            f"Grid too small for tiling: lon={nlon}, lat={nlat} (need at least 2x2)"
        )

    lon_min, lon_max = float(lon.min()), float(lon.max())
    lat_min, lat_max = float(lat.min()), float(lat.max())
    dlon = (lon_max - lon_min) / (nlon - 1)
    dlat = (lat_max - lat_min) / (nlat - 1)

    # Edge-to-edge data span: n cells of d degrees.
    span_x = nlon * dlon
    span_y = nlat * dlat
    cell = max(span_x / total_w, span_y / total_h)

    west = lon_min - dlon / 2
    north = lat_max + dlat / 2
    return GridGeometry(
        total_w=total_w,
        total_h=total_h,
        cell_size=cell,
        west=west,
        north=north,
        east=west + total_w * cell,
        south=north - total_h * cell,
        dlon=dlon,
        dlat=dlat,
        sx_ratio=cell / dlon,
        sy_ratio=cell / dlat,
    )


def lod_grid_geometry(
    ds: xr.Dataset,
    lod_grid: tuple[int, int],
    chunk_px: tuple[int, int],
) -> GridGeometry:
    """Geometry for one LOD given its (cols, rows) grid and the chunk pixel size."""
    cols, rows = lod_grid
    return grid_geometry(ds, cols * chunk_px[0], rows * chunk_px[1])
