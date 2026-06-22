import numpy as np
import rasterio
import xarray

from data_access_service.utils import geotiff_export

# Feature: GeoTIFF I/J grid handler (ticket 8564) - a straight grid collapses to
# 1D losslessly; a tilted (curvilinear) grid must stay 2D so it is not drifted.


def _grid(lat, lon):
    """Wrap 2D lat/lon tables into an (I, J) dataset."""
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
    """Regular grid: every row one latitude, every column one longitude."""
    return _grid(
        lat=[[-30, -30, -30], [-29, -29, -29]],
        lon=[[110, 111, 112], [110, 111, 112]],
    )


def _tilted_grid(n=25):
    """Tilted grid: lat also shifts along a row, lon down a column (n x n so the
    warp has enough control points)."""
    i, j = np.meshgrid(np.arange(n), np.arange(n), indexing="ij")
    lat = -30 + i * 0.05 - j * 0.01
    lon = 110 + j * 0.05 + i * 0.01
    return _grid(lat, lon)


def test_collapsing_a_straight_grid_to_1d_has_no_drift():
    """Straight grid collapses to 1D with no drift."""
    ds = _straight_grid()
    out = geotiff_export.prepare_grid_for_geotiff(ds, "LATITUDE", "LONGITUDE")
    lat_drift = np.max(np.abs(ds["LATITUDE"].values - out["LATITUDE"].values[:, None]))
    lon_drift = np.max(
        np.abs(ds["LONGITUDE"].values - out["LONGITUDE"].values[None, :])
    )
    assert lat_drift == 0
    assert lon_drift == 0


def test_a_tilted_grid_is_not_drifted():
    """The fix keeps a tilted grid 2D, so its coordinates stay put (no drift)."""
    ds = _tilted_grid()
    out = geotiff_export.prepare_grid_for_geotiff(ds, "LATITUDE", "LONGITUDE")
    lat_drift = np.max(np.abs(out["LATITUDE"].values - ds["LATITUDE"].values))
    lon_drift = np.max(np.abs(out["LONGITUDE"].values - ds["LONGITUDE"].values))
    assert lat_drift == 0
    assert lon_drift == 0


def test_curvilinear_warp_puts_data_at_the_right_lonlat(tmp_path):
    """Paint each cell's value = its longitude, warp, then read back at the true
    lon/lat: the value must equal that longitude. A drifted warp would not."""
    ds = _tilted_grid()
    lat2d, lon2d = ds["LATITUDE"].values, ds["LONGITUDE"].values
    ds["UCUR"].values[0] = lon2d  # value of each cell == its longitude

    tif = tmp_path / "warp.tif"
    geotiff_export.curvilinear_slice_to_geotiff(
        ds["UCUR"].isel(TIME=0), tif, lat2d, lon2d
    )

    points = [
        (float(lon2d[i, j]), float(lat2d[i, j])) for i, j in [(5, 5), (10, 15), (20, 8)]
    ]
    with rasterio.open(tif) as r:
        assert r.crs.to_epsg() == 4326
        read_back = [v[0] for v in r.sample(points)]

    for (lon, _lat), value in zip(points, read_back):
        assert abs(value - lon) < 0.05


def _regular_slice():
    """A regular 1D-lat/lon slice; value = lat + lon (ascending lat)."""
    lat = np.linspace(-30, -28, 20)
    lon = np.linspace(110, 113, 24)
    return xarray.DataArray(
        lat[:, None] + lon[None, :],
        dims=("LATITUDE", "LONGITUDE"),
        coords={"LATITUDE": lat, "LONGITUDE": lon},
        name="UCUR",
    )


def test_regular_write_puts_data_at_the_right_lonlat(tmp_path):
    """Same address check for the regular writer: value read at (lon, lat) must
    equal lat + lon, so a transposed or mis-projected raster fails."""
    slice_data = _regular_slice()
    lat, lon = slice_data["LATITUDE"].values, slice_data["LONGITUDE"].values

    tif = tmp_path / "regular.tif"
    geotiff_export.regular_slice_to_geotiff(
        slice_data, tif, "LATITUDE", "LONGITUDE", lat_ascending=True
    )

    points = [(float(lon[j]), float(lat[i])) for i, j in [(3, 3), (10, 15), (18, 5)]]
    with rasterio.open(tif) as r:
        assert r.crs.to_epsg() == 4326
        read_back = [v[0] for v in r.sample(points)]

    for (lon_v, lat_v), value in zip(points, read_back):
        assert abs(value - (lat_v + lon_v)) < 0.05
