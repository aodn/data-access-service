import numpy as np
import xarray

from data_access_service.utils import geotiff_export


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


def test_geotiff_collapses_a_straight_grid_to_1d():
    out = geotiff_export.prepare_grid_for_geotiff(
        _straight_grid(), "LATITUDE", "LONGITUDE"
    )
    assert not geotiff_export.has_ij_dims(out)  # collapsed to 1D
    np.testing.assert_array_equal(out["LATITUDE"].values, [-30, -29])
    np.testing.assert_array_equal(out["LONGITUDE"].values, [110, 111, 112])


def test_geotiff_keeps_a_tilted_grid_2d():
    ds = _tilted_grid()
    out = geotiff_export.prepare_grid_for_geotiff(ds, "LATITUDE", "LONGITUDE")
    assert geotiff_export.has_ij_dims(out)  # stays 2D, coordinates not moved
    np.testing.assert_array_equal(out["LATITUDE"].values, ds["LATITUDE"].values)
