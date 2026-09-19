import numpy as np
import pytest
import xarray as xr

from data_access_service.batch.tiler.zarr_registry import (
    _resolve_zarr_source,
    get_datasource,
    get_store,
    store_registry,
)


def _make_ds(**dims: int) -> xr.Dataset:
    shape = list(dims.values())
    coords = {k: np.arange(v, dtype=float) for k, v in dims.items()}
    return xr.Dataset(
        {"var": xr.DataArray(np.zeros(shape), dims=list(dims.keys()), coords=coords)}
    )


class _FakeZarrSource:
    """Minimal stand-in for aodn_cloud_optimised ZarrDataSource."""

    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds

    def get_data(self, date_start=None, date_end=None, **_kwargs) -> xr.Dataset:
        ds = self.zarr_store
        time_name = (
            "time" if "time" in ds.dims else "TIME" if "TIME" in ds.dims else None
        )
        if time_name is not None and (date_start is not None or date_end is not None):
            return ds.sel({time_name: slice(date_start, date_end)})
        return ds


def _patch_source(monkeypatch, ds: xr.Dataset):
    source = _FakeZarrSource(ds)
    monkeypatch.setattr(
        "data_access_service.batch.tiler.zarr_registry._resolve_zarr_source",
        lambda _url: source,
    )
    return source


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


def test_resolve_zarr_source_passes_chunks_none(monkeypatch):
    """Tiler opens stores with chunks=None so dask graphs are not built at open."""
    from aodn_cloud_optimised.lib import DataQuery

    captured: dict = {}
    source = _FakeZarrSource(_make_ds(time=1, lat=2, lon=3))

    class _FakeGetAodn:
        def get_dataset(self, key, chunks="auto"):
            captured["key"] = key
            captured["chunks"] = chunks
            return source

    # isinstance check in _resolve_zarr_source uses DataQuery.ZarrDataSource
    monkeypatch.setattr(DataQuery, "GetAodn", lambda: _FakeGetAodn())
    monkeypatch.setattr(DataQuery, "ZarrDataSource", _FakeZarrSource)

    result = _resolve_zarr_source("foo")
    assert result is source
    assert captured == {"key": "foo.zarr", "chunks": None}


def test_get_store_raises_when_lat_missing(monkeypatch):
    _patch_source(monkeypatch, _make_ds(time=2, lon=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("no_lat")


def test_get_store_raises_when_lon_missing(monkeypatch):
    _patch_source(monkeypatch, _make_ds(time=2, lat=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("no_lon")


def test_get_store_normalises_coord_names(monkeypatch):
    _patch_source(monkeypatch, _make_ds(TIME=2, LATITUDE=5, LONGITUDE=8))
    result = get_store("uppercase")
    assert "lat" in result.dims
    assert "lon" in result.dims
    assert "time" in result.dims
    assert "LATITUDE" not in result.dims


def test_get_store_sortby_time(monkeypatch):
    ds = _make_ds(time=4, lat=5, lon=8)
    ds = ds.assign_coords(time=np.array([4.0, 1.0, 3.0, 2.0]))
    _patch_source(monkeypatch, ds)
    result = get_store("unsorted")
    assert list(result.time.values) == sorted(result.time.values)


def test_get_datasource_returns_same_source(monkeypatch):
    source = _patch_source(monkeypatch, _make_ds(time=1, lat=3, lon=4))
    get_store("ds")
    assert get_datasource("ds") is source
