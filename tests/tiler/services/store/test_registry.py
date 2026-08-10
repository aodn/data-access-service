import dataclasses

import numpy as np
import pytest
import xarray as xr

from data_access_service.tiler.services.product.product import (
    DataTileConfig,
    Product,
    get_lod_grids,
)
from data_access_service.tiler.services.store import registry
from data_access_service.tiler.services.store.registry import (
    _storage_options,
    get_available_dates,
    get_store,
    is_store_available,
    store_registry,
)


def _make_ds(**dims: int) -> xr.Dataset:
    shape = list(dims.values())
    coords = {k: np.arange(v, dtype=float) for k, v in dims.items()}
    return xr.Dataset(
        {"var": xr.DataArray(np.zeros(shape), dims=list(dims.keys()), coords=coords)}
    )


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


def test_get_store_raises_when_lat_missing(monkeypatch):
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds(time=2, lon=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("s3://test/no_lat.zarr")


def test_get_store_raises_when_lon_missing(monkeypatch):
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds(time=2, lat=10))
    with pytest.raises(ValueError, match="missing lat/lon dims"):
        get_store("s3://test/no_lon.zarr")


def test_get_store_normalises_coord_names(monkeypatch):
    monkeypatch.setattr(
        xr, "open_zarr", lambda *_, **__: _make_ds(TIME=2, LATITUDE=5, LONGITUDE=8)
    )
    result = get_store("s3://test/uppercase.zarr")
    assert "lat" in result.dims
    assert "lon" in result.dims
    assert "time" in result.dims
    assert "LATITUDE" not in result.dims


def test_get_store_sortby_time(monkeypatch):
    ds = _make_ds(time=4, lat=5, lon=8)
    ds = ds.assign_coords(time=np.array([4.0, 1.0, 3.0, 2.0]))
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: ds)
    result = get_store("s3://test/unsorted.zarr")
    assert list(result.time.values) == sorted(result.time.values)


def test_get_available_dates_never_opened_returns_empty_without_opening(monkeypatch):
    """A store nothing has opened yet (never prewarmed, or prewarm failed)
    reports no dates rather than attempting an open — see get_available_dates'
    docstring for why that's safe (every route is gated on prewarm having
    already run for every registered product's store)."""

    def _fail_if_called(*_, **__):
        raise AssertionError("get_available_dates must not open the store")

    monkeypatch.setattr(xr, "open_zarr", _fail_if_called)
    assert get_available_dates("s3://test/never_opened.zarr") == []


def test_get_available_dates_returns_dates_for_already_open_store(monkeypatch):
    ds = _make_ds(time=3, lat=5, lon=8)
    ds = ds.assign_coords(
        time=np.array(
            ["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[ns]"
        )
    )
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: ds)
    get_store("s3://test/opened.zarr")  # simulates prewarm having already run
    assert get_available_dates("s3://test/opened.zarr") == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_is_store_available_false_for_never_opened_store():
    # A cheap cache-membership check, deliberately not opening the store —
    # GET /products' default store_status=available filter must stay fast
    # for a plain listing call rather than risk a blocking open per product.
    assert is_store_available("s3://test/never_opened.zarr") is False


def test_is_store_available_true_after_successful_open(monkeypatch):
    monkeypatch.setattr(
        xr, "open_zarr", lambda *_, **__: _make_ds(time=1, lat=5, lon=8)
    )
    get_store("s3://test/opened.zarr")  # simulates prewarm having already run
    assert is_store_available("s3://test/opened.zarr") is True


def test_is_store_available_false_after_failed_open(monkeypatch):
    def _raise(*_, **__):
        raise ValueError("boom")

    monkeypatch.setattr(xr, "open_zarr", _raise)
    with pytest.raises(ValueError):
        get_store("s3://test/broken.zarr")
    assert is_store_available("s3://test/broken.zarr") is False


def test_get_lod_grids_populates_product(monkeypatch):
    monkeypatch.setattr(
        xr, "open_zarr", lambda *_, **__: _make_ds(time=1, lat=74, lon=102)
    )
    product = Product(id="t1", source_path="s3://test/grids.zarr", variable="var")
    assert product.data_tile.lod_grids == {}
    grids = get_lod_grids(product)
    assert grids
    assert product.data_tile.lod_grids is grids


def test_get_lod_grids_fast_path_skips_store(monkeypatch):
    opened = []
    monkeypatch.setattr(
        xr, "open_zarr", lambda *_, **__: opened.append(1) or _make_ds(lat=5, lon=5)
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


def test_storage_options_s3_defaults_anon():
    opts = _storage_options("s3://bucket/path.zarr")
    assert opts["anon"] is True
    assert "connect_timeout" in opts["config_kwargs"]


def test_storage_options_s3_anon_disabled(monkeypatch):
    monkeypatch.setattr(
        registry,
        "_tiler_config",
        dataclasses.replace(registry._tiler_config, s3_anon=False),
    )
    opts = _storage_options("s3://bucket/path.zarr")
    assert opts["anon"] is False
    assert "connect_timeout" in opts["config_kwargs"]


def test_storage_options_non_s3_returns_empty():
    assert _storage_options("https://example.com/data.zarr") == {}
    assert _storage_options("file:///tmp/data.zarr") == {}
    assert _storage_options("/tmp/data.zarr") == {}
