"""resolve_timestamp_or_404 — the request-time fail-fast guard for a bad date.

A cheap mirror of the check `_fetch_slice_from_store` performs deep in
load_slice (see tests/tiler/services/store/test_slice_loader.py for that one).
Both share `registry.unavailable_date_message`, so the 404 detail here must
match the FileNotFoundError message load_slice raises for the same case.
"""

import pandas as pd
import pytest
from fastapi import HTTPException

from data_access_service.core.tiler_routes.shared import (
    parse_date_or_422,
    resolve_timestamp_or_404,
)
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.store.registry import store_registry


def _seed_metadata(store: str, times: list[str]) -> None:
    """Publish a fake sidecar directly into the registry, bypassing the
    metadata.json file read — the seam this test suite uses in place of the
    old fake-ZarrDataSource monkeypatch."""
    meta = TilerParquetMetadata(
        uuid="u",
        dataset=f"{store}.zarr",
        n_i=1,
        n_j=1,
        lat=[0.0],
        lon=[0.0],
        timestamps=[f"{t}.000000000Z" for t in times],
        variables={"v": TilerVariableMetadata(dtype="float32", attrs={})},
        generated_at="",
    )
    store_registry._publish(store, meta)


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


_PRODUCT = Product(id="p", store="x", variable="v")


def test_parse_date_or_422_rejects_bare_date():
    with pytest.raises(HTTPException) as exc_info:
        parse_date_or_422("2024-06-15")

    assert exc_info.value.status_code == 422


def test_parse_date_or_422_rejects_naive_timestamp():
    with pytest.raises(HTTPException) as exc_info:
        parse_date_or_422("2024-06-15T23:00:00")

    assert exc_info.value.status_code == 422


def test_parse_date_or_422_accepts_z_suffixed_timestamp():
    assert parse_date_or_422("2024-06-15T23:00:00Z") == pd.Timestamp(
        "2024-06-15T23:00:00"
    )


def test_parse_date_or_422_normalizes_offset_to_utc():
    assert parse_date_or_422("2024-06-16T09:00:00+10:00") == pd.Timestamp(
        "2024-06-15T23:00:00"
    )


def test_resolve_timestamp_or_404_passes_for_a_known_date():
    _seed_metadata("x", ["2024-01-15T13:00:00"])
    resolve_timestamp_or_404(_PRODUCT, pd.Timestamp("2024-01-15T13:00:00"))  # no raise


def test_resolve_timestamp_or_404_404s_for_an_unknown_date():
    _seed_metadata("x", ["2024-01-15T13:00:00"])

    with pytest.raises(HTTPException) as exc_info:
        resolve_timestamp_or_404(_PRODUCT, pd.Timestamp("1999-01-01"))

    assert exc_info.value.status_code == 404
    assert "Latest available date is '2024-01-15T13:00:00Z'" in exc_info.value.detail
