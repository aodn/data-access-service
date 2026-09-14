"""snap_read_bbox / xyz tile window helpers."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.tiler.services.store.registry import store_registry
from data_access_service.tiler.services.store.spatial import (
    snap_read_bbox,
    xyz_tile_wgs84_bbox,
)


class _FakeZarrSource:
    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds

    def get_data(self, **_kwargs):
        return self.zarr_store


@pytest.fixture(autouse=True)
def isolate_stores():
    store_registry.clear()
    yield
    store_registry.clear()


def _grid(*, chunks=None) -> xr.Dataset:
    lat = np.linspace(-40.0, -32.0, 8)
    lon = np.linspace(140.0, 148.0, 8)
    da = xr.DataArray(
        np.ones((1, 8, 8), dtype=np.float32),
        dims=["time", "lat", "lon"],
        coords={"time": pd.to_datetime(["2024-01-01"]), "lat": lat, "lon": lon},
    )
    if chunks is not None:
        da.encoding["chunks"] = chunks
    return xr.Dataset({"v": da})


def test_xyz_tile_z0_is_world():
    lon_min, lat_min, lon_max, lat_max = xyz_tile_wgs84_bbox(0, 0, 0)
    assert lon_min == pytest.approx(-180.0)
    assert lon_max == pytest.approx(180.0)
    assert lat_min < 0 < lat_max


def test_snap_read_bbox_no_overlap_returns_none(monkeypatch):
    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        lambda _url: _FakeZarrSource(_grid()),
    )
    # North Atlantic vs an Australia-only grid
    assert (
        snap_read_bbox("s3://b/x.zarr", (-10.0, 40.0, 0.0, 50.0), pad_cells=0) is None
    )


def test_snap_read_bbox_intersects_and_pads(monkeypatch):
    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        lambda _url: _FakeZarrSource(_grid()),
    )
    window = snap_read_bbox("s3://b/x.zarr", (143.0, -36.0, 144.0, -35.0), pad_cells=1)
    assert window is not None
    lon_min, lat_min, lon_max, lat_max = window
    assert lon_min <= 143.0
    assert lon_max >= 144.0
    assert lat_min <= -36.0
    assert lat_max >= -35.0


def test_snap_read_bbox_snaps_to_chunk_edges(monkeypatch):
    ds = _grid(chunks=(1, 4, 4))
    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        lambda _url: _FakeZarrSource(ds),
    )
    # A window inside the first 4×4 chunk must expand to that whole chunk.
    window = snap_read_bbox("s3://b/x.zarr", (140.2, -39.8, 140.4, -39.6), pad_cells=0)
    assert window is not None
    lon_min, lat_min, lon_max, lat_max = window
    lat = ds.lat.values
    lon = ds.lon.values
    assert lon_min == pytest.approx(float(lon[0]))
    assert lon_max == pytest.approx(float(lon[3]))
    assert lat_min == pytest.approx(float(lat[0]))
    assert lat_max == pytest.approx(float(lat[3]))
