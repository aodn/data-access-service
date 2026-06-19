import numpy as np
import xarray

from data_access_service.utils import geotiff_export

# Feature: GeoTIFF I/J grid handler (ticket 8564) - a straight grid collapses to
# 1D losslessly; a tilted (curvilinear) grid must stay 2D so it is not drifted.


def _grid(lat, lon):
    """Wrap hand-written 2D lat/lon tables into an (I, J) dataset."""
    lat, lon = np.array(lat, float), np.array(lon, float)
    return xarray.Dataset(
        {
            "UCUR": (["TIME", "I", "J"], np.zeros((1,) + lat.shape)),
            "LATITUDE": (["I", "J"], lat),
            "LONGITUDE": (["I", "J"], lon),
        },
        coords={"TIME": [0]},
    )


def _straight_grid():
    """Regular grid: every row has one latitude, every column one longitude."""
    return _grid(
        lat=[[-30, -30, -30], [-29, -29, -29]],
        lon=[[110, 111, 112], [110, 111, 112]],
    )


def _tilted_grid():
    """Curvilinear grid: latitude also changes across a row, longitude down a column."""
    return _grid(
        lat=[[-30.0, -30.1, -30.2], [-29.0, -29.1, -29.2]],
        lon=[[110.0, 111.0, 112.0], [110.5, 111.5, 112.5]],
    )


def test_a_straight_grid_has_no_drift():
    ds = _straight_grid()
    drift = geotiff_export.grid_drift(ds["LATITUDE"].values, ds["LONGITUDE"].values)
    assert drift == (0.0, 0.0)


def test_a_tilted_grid_has_drift():
    ds = _tilted_grid()
    lat_drift, lon_drift = geotiff_export.grid_drift(
        ds["LATITUDE"].values, ds["LONGITUDE"].values
    )
    assert lat_drift > 0 and lon_drift > 0


def test_collapsing_a_straight_grid_to_1d_has_no_drift():
    """A straight grid collapses to 1D losslessly, so every original point still
    sits exactly on the new 1D axes (drift == 0)."""
    ds = _straight_grid()
    out = geotiff_export.prepare_grid_for_geotiff(ds, "LATITUDE", "LONGITUDE")

    # how far each original point moved from the new 1D axes
    lat_drift = np.max(np.abs(ds["LATITUDE"].values - out["LATITUDE"].values[:, None]))
    lon_drift = np.max(
        np.abs(ds["LONGITUDE"].values - out["LONGITUDE"].values[None, :])
    )
    assert lat_drift == 0
    assert lon_drift == 0


def test_a_tilted_grid_is_not_drifted():
    """The bug collapsed a tilted grid to 1D and moved its points. The fix keeps
    the grid 2D, so every coordinate stays put (drift == 0)."""
    ds = _tilted_grid()
    out = geotiff_export.prepare_grid_for_geotiff(ds, "LATITUDE", "LONGITUDE")

    # how far each coordinate moved after processing
    lat_drift = np.max(np.abs(out["LATITUDE"].values - ds["LATITUDE"].values))
    lon_drift = np.max(np.abs(out["LONGITUDE"].values - ds["LONGITUDE"].values))
    assert lat_drift == 0
    assert lon_drift == 0
