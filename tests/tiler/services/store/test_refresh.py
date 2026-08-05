"""TTL-triggered refresh is bounded and jittered.

Previously one raw thread was spawned per expired store. That was fine at two
stores; at sixty it is a stampede — every store reopening from S3 at once,
uncapped, competing with live tile requests for the same connection pool. And
because prewarm opens them all together they share a deadline, so the stampede
is periodic rather than incidental.
"""

import threading
import time

import numpy as np
import pytest
import xarray as xr

from data_access_service.tiler.services.store import registry
from data_access_service.tiler.services.store.registry import StoreRegistry


def _make_ds() -> xr.Dataset:
    return xr.Dataset(
        {
            "var": xr.DataArray(
                np.zeros((2, 2)),
                dims=("lat", "lon"),
                coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0]},
            )
        }
    )


def test_refresh_uses_the_bounded_pool_not_a_raw_thread(monkeypatch):
    """A raw Thread per expired store is exactly the unbounded behaviour this
    replaced, so spawning one is a regression even if it happens to work."""
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())
    monkeypatch.setattr(
        threading,
        "Thread",
        lambda *a, **k: pytest.fail("refresh must go through the bounded executor"),
    )
    submitted: list[str] = []
    monkeypatch.setattr(
        registry._REFRESH_EXECUTOR,
        "submit",
        lambda fn, url: submitted.append(url),
    )

    store = StoreRegistry(ttl=0.0)  # everything is immediately stale
    store.get("s3://b/a.zarr")
    store.get("s3://b/a.zarr")

    assert submitted == ["s3://b/a.zarr"]


def test_refresh_concurrency_never_exceeds_the_configured_bound(monkeypatch):
    peak = {"current": 0, "max": 0}
    lock = threading.Lock()

    def slow_open(*_, **__):
        with lock:
            peak["current"] += 1
            peak["max"] = max(peak["max"], peak["current"])
        time.sleep(0.05)
        with lock:
            peak["current"] -= 1
        return _make_ds()

    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())
    store = StoreRegistry(ttl=0.0)
    urls = [f"s3://b/{i}.zarr" for i in range(20)]
    for url in urls:
        store.get(url)

    monkeypatch.setattr(xr, "open_zarr", slow_open)
    for url in urls:
        store.get(url)  # each is stale, so each queues a refresh

    deadline = time.monotonic() + 10
    while store._refreshing and time.monotonic() < deadline:
        time.sleep(0.01)

    bound = registry._REFRESH_EXECUTOR._max_workers
    assert peak["max"] <= bound, f"{peak['max']} concurrent refreshes exceeds {bound}"
    assert peak["max"] > 0, "no refresh actually ran"


def test_one_store_queues_at_most_one_refresh(monkeypatch):
    """The _refreshing guard means concurrent requests for the same stale store
    do not each queue their own reopen."""
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())
    submitted: list[str] = []
    monkeypatch.setattr(
        registry._REFRESH_EXECUTOR, "submit", lambda fn, url: submitted.append(url)
    )

    store = StoreRegistry(ttl=0.0)
    store.get("s3://b/a.zarr")
    for _ in range(10):
        store.get("s3://b/a.zarr")

    assert submitted == ["s3://b/a.zarr"]


def test_stale_store_is_served_immediately_during_refresh(monkeypatch):
    """Requests never block on freshness — the point of refreshing in the
    background rather than on the request path."""
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())
    monkeypatch.setattr(registry._REFRESH_EXECUTOR, "submit", lambda fn, url: None)

    store = StoreRegistry(ttl=0.0)
    first = store.get("s3://b/a.zarr")
    assert store.get("s3://b/a.zarr") is first


def test_jitter_spreads_deadlines_across_stores(monkeypatch):
    """Stores opened in one startup burst must not share an expiry instant."""
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())

    store = StoreRegistry(ttl=600.0)
    for i in range(30):
        store.get(f"s3://b/{i}.zarr")

    jitters = set(store._ttl_jitter.values())
    assert len(jitters) > 1, "every store drew the same jitter"
    assert all(0.0 <= j <= 600.0 * registry._REFRESH_JITTER_FRACTION for j in jitters)


def test_jitter_is_bounded_so_freshness_policy_is_not_changed(monkeypatch):
    """HF-radar datasets update frequently; jitter is for spreading load, not
    for quietly extending the TTL."""
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())

    store = StoreRegistry(ttl=600.0)
    store.get("s3://b/a.zarr")

    assert store._ttl_jitter["s3://b/a.zarr"] <= 60.0


def test_clear_drops_jitter_state(monkeypatch):
    monkeypatch.setattr(xr, "open_zarr", lambda *_, **__: _make_ds())
    store = StoreRegistry(ttl=600.0)
    store.get("s3://b/a.zarr")

    store.clear()

    assert store._ttl_jitter == {}
