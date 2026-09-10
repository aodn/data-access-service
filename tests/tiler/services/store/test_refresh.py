"""Cron-triggered refresh: ``refresh_all`` re-opens every currently-valid
store sequentially, is never triggered from the request path, and does not
let one store's failure stop the sweep.
"""

import threading

import numpy as np
import xarray as xr

from data_access_service.tiler.services.store.registry import StoreRegistry


def _make_ds() -> xr.Dataset:
    return xr.Dataset(
        {
            "var": xr.DataArray(
                np.zeros((1, 2, 2)),
                dims=("time", "lat", "lon"),
                coords={
                    "time": np.array([0], dtype="datetime64[ns]"),
                    "lat": [0.0, 1.0],
                    "lon": [0.0, 1.0],
                },
            )
        }
    )


class _FakeZarrSource:
    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds


def _patch_resolve(monkeypatch, factory):
    """``factory`` is a one-arg callable (store_url) returning a Dataset (or raises)."""

    def resolve(url: str):
        return _FakeZarrSource(factory(url))

    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        resolve,
    )


def test_request_path_never_refreshes_an_already_open_store(monkeypatch):
    opens: list[str] = []

    def factory(url):
        opens.append(url)
        return _make_ds()

    _patch_resolve(monkeypatch, factory)
    store = StoreRegistry()

    first = store.get_datasource("s3://b/a.zarr")
    for _ in range(10):
        assert store.get_datasource("s3://b/a.zarr") is first

    assert opens == ["s3://b/a.zarr"]  # only the initial open, never a refresh


def test_refresh_all_reopens_every_published_store(monkeypatch):
    opens: list[str] = []

    def factory(url):
        opens.append(url)
        return _make_ds()

    _patch_resolve(monkeypatch, factory)
    store = StoreRegistry()
    urls = [f"s3://b/{i}.zarr" for i in range(5)]
    for url in urls:
        store.get(url)
    opens.clear()

    store.refresh_all()

    assert opens == urls


def test_refresh_all_publishes_a_new_source_object(monkeypatch):
    _patch_resolve(monkeypatch, lambda url: _make_ds())
    store = StoreRegistry()
    store.get("s3://b/a.zarr")
    first = store.get_datasource("s3://b/a.zarr")

    store.refresh_all()

    assert store.get_datasource("s3://b/a.zarr") is not first


def test_refresh_all_is_sequential_not_concurrent(monkeypatch):
    peak = {"current": 0, "max": 0}
    lock = threading.Lock()

    def factory(url):
        with lock:
            peak["current"] += 1
            peak["max"] = max(peak["max"], peak["current"])
        with lock:
            peak["current"] -= 1
        return _make_ds()

    _patch_resolve(monkeypatch, factory)
    store = StoreRegistry()
    urls = [f"s3://b/{i}.zarr" for i in range(20)]
    for url in urls:
        store.get(url)

    store.refresh_all()

    assert peak["max"] == 1


def test_one_store_failure_does_not_stop_the_sweep(monkeypatch):
    refreshing = {"active": False}

    def factory(url):
        if refreshing["active"] and url == "s3://b/broken.zarr":
            raise RuntimeError("s3 down")
        return _make_ds()

    _patch_resolve(monkeypatch, factory)
    store = StoreRegistry()
    store.get("s3://b/broken.zarr")
    store.get("s3://b/healthy.zarr")
    healthy_first = store.get_datasource("s3://b/healthy.zarr")

    refreshing["active"] = True
    store.refresh_all()  # must not raise

    # The failed refresh keeps serving the last-known-good handle...
    assert store.get_datasource("s3://b/broken.zarr") is not None
    # ...while the other store's refresh still went through.
    assert store.get_datasource("s3://b/healthy.zarr") is not healthy_first


def test_refresh_all_skips_stores_never_opened():
    store = StoreRegistry()
    store.refresh_all()  # nothing published yet; must not raise
    assert store._stores == {}


def test_clear_drops_published_stores(monkeypatch):
    _patch_resolve(monkeypatch, lambda url: _make_ds())
    store = StoreRegistry()
    store.get("s3://b/a.zarr")

    store.clear()

    assert store._stores == {}
