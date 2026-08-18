"""resolve_timestamp_or_404 — the request-time fail-fast guard for a bad date.

A cheap mirror of the check `_fetch_slice_from_store` performs deep in
load_slice (see tests/tiler/services/store/test_slice_loader.py for that one).
Both share `registry.unavailable_date_message`, so the 404 detail here must
match the FileNotFoundError message load_slice raises for the same case.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi import HTTPException

from data_access_service.core.tiler_routes.shared import resolve_timestamp_or_404
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.store.registry import store_registry


class _FakeZarrSource:
    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds


def _patch_source(monkeypatch, ds: xr.Dataset):
    monkeypatch.setattr(
        "data_access_service.tiler.services.store.registry._resolve_zarr_source",
        lambda _url: _FakeZarrSource(ds),
    )


@pytest.fixture(autouse=True)
def isolate_caches():
    store_registry.clear()
    yield
    store_registry.clear()


@pytest.fixture(autouse=True)
def resolve_timestamp_mock():
    """Override conftest's default mock: this file tests resolve_timestamp_or_404
    itself against a fake store, so the real registry call must go through."""
    yield None


def _ds_with_time(times: list[str]) -> xr.Dataset:
    t = pd.to_datetime(times)
    return xr.Dataset(
        {
            "v": xr.DataArray(
                np.zeros((len(times), 1, 1), dtype=np.float32),
                dims=["time", "lat", "lon"],
                coords={"time": t, "lat": [0.0], "lon": [0.0]},
            )
        }
    )


_PRODUCT = Product(id="p", source_path="s3://b/x.zarr", variable="v")


def test_resolve_timestamp_or_404_passes_for_a_known_date(monkeypatch):
    _patch_source(monkeypatch, _ds_with_time(["2024-01-15T13:00:00"]))
    resolve_timestamp_or_404(_PRODUCT, pd.Timestamp("2024-01-15T13:00:00"))  # no raise


def test_resolve_timestamp_or_404_404s_for_an_unknown_date(monkeypatch):
    _patch_source(monkeypatch, _ds_with_time(["2024-01-15T13:00:00"]))

    with pytest.raises(HTTPException) as exc_info:
        resolve_timestamp_or_404(_PRODUCT, pd.Timestamp("1999-01-01"))

    assert exc_info.value.status_code == 404
    assert "Latest available date is '2024-01-15T13:00:00Z'" in exc_info.value.detail
