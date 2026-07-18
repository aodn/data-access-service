"""Square-cell NW-anchored LOD grid geometry (product/grid_geometry.py).

These invariants are the reason cv2 exists: one isotropic cell size per LOD so
the grid is exactly representable as an OGC TileMatrixSet, anchored at the
data's NW edge, with any aspect mismatch pushed to masked east/south padding.
"""

import numpy as np
import pytest
import xarray as xr

from data_access_service.tiler.services.product.grid_geometry import (
    grid_geometry,
    lod_grid_geometry,
)
from data_access_service.tiler.services.rendering.kernels import (
    resample_variables_to_grid,
)


def _ds(nlon=20, nlat=10, lon0=100.0, dlon=0.5, lat0=-10.0, dlat=0.5):
    lon = lon0 + np.arange(nlon) * dlon
    lat = lat0 - np.arange(nlat) * dlat  # north → south
    data = np.random.rand(nlat, nlon).astype("float32")
    return xr.Dataset(
        {"v": xr.DataArray(data, dims=["lat", "lon"], coords={"lat": lat, "lon": lon})}
    )


def test_cell_size_is_isotropic_and_covers_both_axes():
    ds = _ds(nlon=20, nlat=10)  # lon span 10°, lat span 5°
    geom = grid_geometry(ds, 16, 16)
    # One cell size, sized by the axis that needs the larger cell.
    assert geom.cell_size == pytest.approx(max(20 * 0.5 / 16, 10 * 0.5 / 16))
    # The grid always covers the data on both axes (edge-to-edge).
    assert geom.east >= float(ds.lon.max()) + geom.dlon / 2
    assert geom.south <= float(ds.lat.min()) - geom.dlat / 2


def test_grid_is_nw_anchored_on_data_edges():
    ds = _ds()
    geom = grid_geometry(ds, 16, 16)
    assert geom.west == pytest.approx(float(ds.lon.min()) - geom.dlon / 2)
    assert geom.north == pytest.approx(float(ds.lat.max()) + geom.dlat / 2)
    # Far edges follow origin + total px · cell — the TMS-implied extent.
    assert geom.east == pytest.approx(geom.west + 16 * geom.cell_size)
    assert geom.south == pytest.approx(geom.north - 16 * geom.cell_size)


def test_binding_axis_fills_exactly():
    ds = _ds(nlon=20, nlat=10)  # lon binds a square grid
    geom = grid_geometry(ds, 16, 16)
    # Binding axis: grid extent equals the data's edge-to-edge span exactly.
    assert 16 * geom.cell_size == pytest.approx(20 * geom.dlon)


def test_lod_grid_geometry_uses_grid_times_chunk():
    ds = _ds()
    geom = lod_grid_geometry(ds, (3, 2), (240, 192))
    assert geom.total_w == 720
    assert geom.total_h == 384


def test_rejects_degenerate_grids():
    with pytest.raises(ValueError):
        grid_geometry(_ds(nlon=1, nlat=10), 8, 8)


def test_resample_masks_past_the_data_south_edge():
    # Wide-but-short data on a square grid: rows past the data's south coverage
    # must come back NaN (partially-filled edge), not stretched data.
    ds = _ds(nlon=32, nlat=16, dlon=1.0, dlat=1.0)
    (out,) = resample_variables_to_grid(ds, ["v"], 8, 8)
    # cell = max(32/8, 16/8) = 4 source steps → 16 rows cover 16/4 = 4 grid rows.
    assert np.isfinite(out[:4]).all()
    assert np.isnan(out[4:]).all()
    # All columns covered on the binding axis.
    assert np.isfinite(out[:4, :]).all()


def test_resample_upsamples_full_coverage_when_aspect_matches():
    ds = _ds(nlon=16, nlat=16, dlon=1.0, dlat=1.0)
    (out,) = resample_variables_to_grid(ds, ["v"], 32, 32)
    assert np.isfinite(out).all()
