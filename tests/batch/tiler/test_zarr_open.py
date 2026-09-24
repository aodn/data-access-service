"""open_store: outcome reporting and bounded retry.

open_store reports what happened and decides nothing; the generator skips a
store that failed and carries on with the next.
"""

import numpy as np
import pytest
import xarray as xr

from data_access_service.batch.tiler import zarr_registry as registry
from data_access_service.batch.tiler.zarr_registry import (
    NoTimeDimensionError,
    NotGriddedStoreError,
    close_all_stores,
    open_store,
)


def _make_ds(**dims: int) -> xr.Dataset:
    shape = list(dims.values())
    coords = {k: np.arange(v, dtype=float) for k, v in dims.items()}
    return xr.Dataset(
        {"var": xr.DataArray(np.zeros(shape), dims=list(dims.keys()), coords=coords)}
    )


class _FakeZarrSource:
    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds


@pytest.fixture(autouse=True)
def clear_stores():
    close_all_stores()
    yield
    close_all_stores()


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Retry delays are real seconds in production; tests only assert the count."""
    monkeypatch.setattr(registry, "_OPEN_BACKOFF_SECONDS", 0.0)


def _patch_resolve(monkeypatch, behaviour):
    """Stub ``_resolve_zarr_source`` dispatching on store.

    Values are datasets, raisers (BaseException instances), or callables.
    """
    calls: list[str] = []

    def resolve(store: str):
        calls.append(store)
        result = behaviour[store]
        if isinstance(result, BaseException):
            raise result
        if callable(result):
            result = result(store)
        return _FakeZarrSource(result)

    monkeypatch.setattr(
        "data_access_service.batch.tiler.zarr_registry._resolve_zarr_source",
        resolve,
    )
    return calls


# --- outcome reporting ------------------------------------------------------


def test_successful_open_returns_none(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"a": _make_ds(time=2, lat=4, lon=4)})

    assert open_store("a") is None
    assert calls == ["a"]


def test_failure_is_returned_not_swallowed(monkeypatch):
    boom = RuntimeError("s3 unavailable")
    _patch_resolve(monkeypatch, {"bad": boom})

    assert open_store("bad") is boom


# --- confirmed non-grid stores ---------------------------------------------


def test_missing_lat_lon_yields_not_gridded_store_error(monkeypatch):
    _patch_resolve(monkeypatch, {"flat": _make_ds(time=2, lon=4)})

    assert isinstance(open_store("flat"), NotGriddedStoreError)


def test_not_gridded_store_error_is_a_value_error():
    assert issubclass(NotGriddedStoreError, ValueError)


def test_non_grid_store_is_not_retried(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"flat": _make_ds(time=2, lon=4)})

    open_store("flat")

    assert calls == ["flat"]


def test_non_grid_store_logs_at_info_without_traceback(monkeypatch, caplog):
    _patch_resolve(monkeypatch, {"flat": _make_ds(time=2, lon=4)})

    with caplog.at_level("INFO"):
        open_store("flat")

    skip_records = [r for r in caplog.records if "not a lat/lon grid" in r.message]
    assert skip_records
    assert all(r.levelname == "INFO" for r in skip_records)
    assert all(r.exc_info is None for r in skip_records)


# --- stores that do not exist ----------------------------------------------


def test_missing_store_yields_file_not_found(monkeypatch):
    _patch_resolve(monkeypatch, {"gone": FileNotFoundError("No such file")})

    assert isinstance(open_store("gone"), FileNotFoundError)


def test_missing_store_is_not_retried(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"gone": FileNotFoundError("No such file")})

    open_store("gone")

    assert calls == ["gone"]


def test_missing_store_logs_at_warning(monkeypatch, caplog):
    _patch_resolve(monkeypatch, {"gone": FileNotFoundError("No such file")})

    with caplog.at_level("INFO"):
        open_store("gone")

    records = [r for r in caplog.records if "does not exist" in r.message]
    assert records
    assert all(r.levelname == "WARNING" for r in records)


# --- no time dimension -------------------------------------------------------


def test_missing_time_dimension_yields_no_time_dimension_error(monkeypatch):
    _patch_resolve(monkeypatch, {"notime": _make_ds(lat=4, lon=4)})

    assert isinstance(open_store("notime"), NoTimeDimensionError)


def test_no_time_dimension_store_is_not_retried(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"notime": _make_ds(lat=4, lon=4)})

    open_store("notime")

    assert calls == ["notime"]


# --- bounded retry ----------------------------------------------------------


def test_operational_failure_is_retried_a_bounded_number_of_times(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"flaky": RuntimeError("timeout")})

    outcome = open_store("flaky")

    assert len(calls) == registry._OPEN_MAX_ATTEMPTS
    assert isinstance(outcome, RuntimeError)


def test_retry_succeeds_after_a_transient_failure(monkeypatch):
    attempts = {"n": 0}

    def flaky(_store):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("transient")
        return _make_ds(time=2, lat=4, lon=4)

    _patch_resolve(monkeypatch, {"flaky": flaky})

    assert open_store("flaky") is None
    assert attempts["n"] == 2


def test_backoff_grows_between_attempts(monkeypatch):
    delays: list[float] = []
    monkeypatch.setattr(registry, "_OPEN_BACKOFF_SECONDS", 1.0)
    monkeypatch.setattr(registry.time, "sleep", delays.append)
    _patch_resolve(monkeypatch, {"flaky": RuntimeError("timeout")})

    open_store("flaky")

    # One sleep fewer than attempts — no wait after the final failure.
    assert delays == [1.0, 2.0]


def test_failed_open_is_not_cached_so_a_later_request_retries(monkeypatch):
    attempts = {"n": 0}

    def flaky(_store):
        attempts["n"] += 1
        if attempts["n"] <= registry._OPEN_MAX_ATTEMPTS:
            raise RuntimeError("s3 down")
        return _make_ds(time=2, lat=4, lon=4)

    _patch_resolve(monkeypatch, {"recovers": flaky})

    assert isinstance(open_store("recovers"), RuntimeError)
    # S3 comes back; the next request opens it without any intervention.
    assert registry.get_store("recovers") is not None


def test_close_store_drops_the_cached_handle(monkeypatch):
    calls = _patch_resolve(monkeypatch, {"a": _make_ds(time=2, lat=4, lon=4)})
    open_store("a")

    registry.close_store("a")
    open_store("a")

    assert calls == ["a", "a"]
