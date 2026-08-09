"""Prewarm outcome reporting and bounded retry.

Prewarm reports what happened per URL and decides nothing; only the caller can
tell "one site unreachable" from "the tiler is down".
"""

import numpy as np
import pytest
import xarray as xr

from data_access_service.tiler.services.store import registry
from data_access_service.tiler.services.store.registry import (
    NotGriddedStoreError,
    StoreRegistry,
    prewarm_stores,
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


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Retry delays are real seconds in production; tests only assert the count."""
    monkeypatch.setattr(registry, "_PREWARM_BACKOFF_SECONDS", 0.0)


def _open_zarr(behaviour):
    """Build an open_zarr stub dispatching on URL. Values are datasets or raisers."""
    calls: list[str] = []

    def opener(url, *_, **__):
        calls.append(url)
        result = behaviour[url]
        if isinstance(result, BaseException):
            raise result
        if callable(result):
            return result(url)
        return result

    return opener, calls


# --- outcome reporting ------------------------------------------------------


@pytest.mark.asyncio
async def test_successful_prewarm_reports_none_per_url(monkeypatch):
    opener, calls = _open_zarr(
        {
            "s3://b/a.zarr": _make_ds(time=2, lat=4, lon=4),
            "s3://b/b.zarr": _make_ds(time=2, lat=4, lon=4),
        }
    )
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/a.zarr", "s3://b/b.zarr"])

    assert outcomes == {"s3://b/a.zarr": None, "s3://b/b.zarr": None}
    assert sorted(calls) == ["s3://b/a.zarr", "s3://b/b.zarr"]


@pytest.mark.asyncio
async def test_failures_are_returned_not_swallowed(monkeypatch):
    boom = RuntimeError("s3 unavailable")
    opener, _ = _open_zarr(
        {"s3://b/ok.zarr": _make_ds(time=2, lat=4, lon=4), "s3://b/bad.zarr": boom}
    )
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/ok.zarr", "s3://b/bad.zarr"])

    assert outcomes["s3://b/ok.zarr"] is None
    assert outcomes["s3://b/bad.zarr"] is boom


@pytest.mark.asyncio
async def test_one_bad_url_does_not_block_the_others(monkeypatch):
    opener, calls = _open_zarr(
        {
            "s3://b/bad.zarr": RuntimeError("nope"),
            "s3://b/ok1.zarr": _make_ds(time=2, lat=4, lon=4),
            "s3://b/ok2.zarr": _make_ds(time=2, lat=4, lon=4),
        }
    )
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(
        ["s3://b/bad.zarr", "s3://b/ok1.zarr", "s3://b/ok2.zarr"]
    )

    assert outcomes["s3://b/ok1.zarr"] is None
    assert outcomes["s3://b/ok2.zarr"] is None
    assert isinstance(outcomes["s3://b/bad.zarr"], RuntimeError)


# --- confirmed non-grid stores ---------------------------------------------


@pytest.mark.asyncio
async def test_missing_lat_lon_yields_not_gridded_store_error(monkeypatch):
    opener, _ = _open_zarr({"s3://b/flat.zarr": _make_ds(time=2, lon=4)})
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/flat.zarr"])

    assert isinstance(outcomes["s3://b/flat.zarr"], NotGriddedStoreError)


def test_not_gridded_store_error_is_a_value_error():
    """Existing callers catch ValueError; the typed subclass keeps them working."""
    assert issubclass(NotGriddedStoreError, ValueError)


@pytest.mark.asyncio
async def test_non_grid_store_is_not_retried(monkeypatch):
    """The store answered. Retrying will not change the answer, and sixty
    legitimate skips retried three times each would be pure startup cost."""
    opener, calls = _open_zarr({"s3://b/flat.zarr": _make_ds(time=2, lon=4)})
    monkeypatch.setattr(xr, "open_zarr", opener)

    await prewarm_stores(["s3://b/flat.zarr"])

    assert calls == ["s3://b/flat.zarr"]


@pytest.mark.asyncio
async def test_non_grid_store_logs_at_info_without_traceback(monkeypatch, caplog):
    opener, _ = _open_zarr({"s3://b/flat.zarr": _make_ds(time=2, lon=4)})
    monkeypatch.setattr(xr, "open_zarr", opener)

    with caplog.at_level("INFO"):
        await prewarm_stores(["s3://b/flat.zarr"])

    skip_records = [r for r in caplog.records if "not a lat/lon grid" in r.message]
    assert skip_records
    assert all(r.levelname == "INFO" for r in skip_records)
    assert all(r.exc_info is None for r in skip_records)


# --- stores that do not exist ----------------------------------------------


@pytest.mark.asyncio
async def test_missing_store_yields_file_not_found(monkeypatch):
    opener, _ = _open_zarr({"s3://b/gone.zarr": FileNotFoundError("No such file")})
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/gone.zarr"])

    assert isinstance(outcomes["s3://b/gone.zarr"], FileNotFoundError)


@pytest.mark.asyncio
async def test_missing_store_is_not_retried(monkeypatch):
    """The bucket answered "not there", which is as final as a non-grid answer.
    Retrying it three times per missing store is pure startup cost."""
    opener, calls = _open_zarr({"s3://b/gone.zarr": FileNotFoundError("No such file")})
    monkeypatch.setattr(xr, "open_zarr", opener)

    await prewarm_stores(["s3://b/gone.zarr"])

    assert calls == ["s3://b/gone.zarr"]


@pytest.mark.asyncio
async def test_missing_store_logs_at_warning(monkeypatch, caplog):
    """Unlike a non-grid store, an absent one is never intentional — usually an
    upstream rename the catalogue has not caught up with."""
    opener, _ = _open_zarr({"s3://b/gone.zarr": FileNotFoundError("No such file")})
    monkeypatch.setattr(xr, "open_zarr", opener)

    with caplog.at_level("INFO"):
        await prewarm_stores(["s3://b/gone.zarr"])

    records = [r for r in caplog.records if "does not exist" in r.message]
    assert records
    assert all(r.levelname == "WARNING" for r in records)


# --- bounded retry ----------------------------------------------------------


@pytest.mark.asyncio
async def test_operational_failure_is_retried_a_bounded_number_of_times(monkeypatch):
    opener, calls = _open_zarr({"s3://b/flaky.zarr": RuntimeError("timeout")})
    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/flaky.zarr"])

    assert len(calls) == registry._PREWARM_MAX_ATTEMPTS
    assert isinstance(outcomes["s3://b/flaky.zarr"], RuntimeError)


@pytest.mark.asyncio
async def test_retry_succeeds_after_a_transient_failure(monkeypatch):
    attempts = {"n": 0}

    def opener(url, *_, **__):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("transient")
        return _make_ds(time=2, lat=4, lon=4)

    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/flaky.zarr"])

    assert outcomes["s3://b/flaky.zarr"] is None
    assert attempts["n"] == 2


@pytest.mark.asyncio
async def test_backoff_grows_between_attempts(monkeypatch):
    delays: list[float] = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(registry, "_PREWARM_BACKOFF_SECONDS", 1.0)
    monkeypatch.setattr(registry.anyio, "sleep", fake_sleep)
    opener, _ = _open_zarr({"s3://b/flaky.zarr": RuntimeError("timeout")})
    monkeypatch.setattr(xr, "open_zarr", opener)

    await prewarm_stores(["s3://b/flaky.zarr"])

    # One sleep fewer than attempts — no wait after the final failure.
    assert delays == [1.0, 2.0]


@pytest.mark.asyncio
async def test_prewarm_summary_counts_each_outcome_category(monkeypatch, caplog):
    opener, _ = _open_zarr(
        {
            "s3://b/ok.zarr": _make_ds(time=2, lat=4, lon=4),
            "s3://b/flat.zarr": _make_ds(time=2, lon=4),
            "s3://b/bad.zarr": RuntimeError("nope"),
        }
    )
    monkeypatch.setattr(xr, "open_zarr", opener)

    with caplog.at_level("INFO"):
        await prewarm_stores(["s3://b/ok.zarr", "s3://b/flat.zarr", "s3://b/bad.zarr"])

    summary = [r for r in caplog.records if "Store prewarm complete" in r.message]
    assert len(summary) == 1
    assert "1 opened, 1 not gridded, 1 unresolved (of 3)" in summary[0].getMessage()


@pytest.mark.asyncio
async def test_failed_open_is_not_cached_so_a_later_request_retries(monkeypatch):
    """The per-URL future is discarded in `finally`, so an unresolved store
    recovers on its own on the first real request. That is what makes keeping a
    degraded product registered safe rather than permanently broken."""
    attempts = {"n": 0}

    def opener(url, *_, **__):
        attempts["n"] += 1
        if attempts["n"] <= registry._PREWARM_MAX_ATTEMPTS:
            raise RuntimeError("s3 down")
        return _make_ds(time=2, lat=4, lon=4)

    monkeypatch.setattr(xr, "open_zarr", opener)

    outcomes = await prewarm_stores(["s3://b/recovers.zarr"])
    assert isinstance(outcomes["s3://b/recovers.zarr"], RuntimeError)

    # S3 comes back; the next real request opens it without any intervention.
    assert store_registry.get("s3://b/recovers.zarr") is not None


@pytest.mark.asyncio
async def test_prewarm_of_an_empty_url_list_is_a_no_op():
    assert await StoreRegistry(ttl=60).prewarm([]) == {}
