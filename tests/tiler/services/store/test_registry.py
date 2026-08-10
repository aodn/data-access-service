import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.tiler.services.product.product import (
    DataTileConfig,
    Product,
    get_lod_grids,
)
from data_access_service.tiler.services.store.registry import (
    _dataset_key_from_url,
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
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        lambda _url: source,
    )
    return source


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


def test_dataset_key_from_url():
    assert _dataset_key_from_url("s3://aodn-cloud-optimised/foo.zarr/") == "foo.zarr"
    assert _dataset_key_from_url("s3://bucket/prefix/bar.zarr") == "bar.zarr"


def test_dataset_key_from_url_rejects_non_zarr():
    with pytest.raises(ValueError, match="\\.zarr"):
        _dataset_key_from_url("s3://bucket/foo.parquet")


def test_get_store_raises_when_lat_missing(monkeypatch):
    _patch_source(monkeypatch, _make_ds(time=2, lon=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("s3://test/no_lat.zarr")


def test_get_store_raises_when_lon_missing(monkeypatch):
    _patch_source(monkeypatch, _make_ds(time=2, lat=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("s3://test/no_lon.zarr")


def test_get_store_normalises_coord_names(monkeypatch):
    _patch_source(monkeypatch, _make_ds(TIME=2, LATITUDE=5, LONGITUDE=8))
    result = get_store("s3://test/uppercase.zarr")
    assert "lat" in result.dims
    assert "lon" in result.dims
    assert "time" in result.dims
    assert "LATITUDE" not in result.dims


def test_get_store_sortby_time(monkeypatch):
    ds = _make_ds(time=4, lat=5, lon=8)
    ds = ds.assign_coords(time=np.array([4.0, 1.0, 3.0, 2.0]))
    _patch_source(monkeypatch, ds)
    result = get_store("s3://test/unsorted.zarr")
    assert list(result.time.values) == sorted(result.time.values)


def test_get_datasource_returns_same_source(monkeypatch):
    source = _patch_source(monkeypatch, _make_ds(time=1, lat=3, lon=4))
    get_store("s3://test/ds.zarr")
    assert get_datasource("s3://test/ds.zarr") is source


def test_get_lod_grids_populates_product(monkeypatch):
    _patch_source(monkeypatch, _make_ds(time=1, lat=74, lon=102))
    product = Product(id="t1", source_path="s3://test/grids.zarr", variable="var")
    assert product.data_tile.lod_grids == {}
    grids = get_lod_grids(product)
    assert grids
    assert product.data_tile.lod_grids is grids


def test_get_lod_grids_fast_path_skips_store(monkeypatch):
    opened = []

    def resolve(_url):
        opened.append(1)
        return _FakeZarrSource(_make_ds(lat=5, lon=5))

    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        resolve,
    )
    product = Product(
        id="t2",
        source_path="s3://test/preset.zarr",
        variable="var",
        data_tile=DataTileConfig(lod_grids={1: (2, 2)}),
    )
    grids = get_lod_grids(product)
    assert grids == {1: (2, 2)}
    assert not opened
