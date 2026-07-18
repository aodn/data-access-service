"""Coverage discovery (product/coverage.py): the browser-safe document that
feeds ogcapi-java's coverage catalog. Pins the shape, the datetime↔dateKey
mapping rule (first timestamp per local date), and — critically — that no
source path ever crosses the trust boundary."""

import json

import numpy as np
import pytest
import xarray as xr

import data_access_service.tiler.services.product.coverage as coverage_module
from data_access_service.tiler.services.product.coverage import (
    build_coverage_discovery,
)
from data_access_service.tiler.services.product.product import (
    CoverageConfig,
    PortalAssociation,
    Product,
)
from data_access_service.tiler.services.product.registry import PRODUCTS

_COLLECTION = "2ffccdad-1197-4e41-b412-a9033517cfb2"
_SOURCE = "s3://bucket/store.zarr"


def _make_store(categorical: bool = False) -> xr.Dataset:
    lat = np.linspace(-8.0, -48.0, 20)  # north → south
    lon = np.linspace(96.0, 174.0, 40)
    data = np.random.rand(20, 40).astype("float32")
    da = xr.DataArray(data, dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
    if categorical:
        da.attrs["flag_values"] = [0, 1, 2]
        da.attrs["flag_meanings"] = "none weak strong"
    return xr.Dataset({"VAR": da})


def _product(product_id="cov1", default=True, variable="VAR") -> Product:
    return Product(
        id=product_id,
        source_path=_SOURCE,
        variable=variable,
        lod_grids={1: (2, 2)},
        chunk_px=(8, 8),
        padding=1,
        title="Test coverage product",
        portal=PortalAssociation(
            collection_id=_COLLECTION, dataset_key="test_dataset", default=default
        ),
        coverage=CoverageConfig(enabled=True, tile_matrix_set_id="tms_test"),
    )


@pytest.fixture
def seeded(monkeypatch):
    ds = _make_store()
    date_index = {
        "2026-07-16": [np.datetime64("2026-07-15T14:00:00")],  # 2026-07-16 AEST
        "2026-07-17": [
            np.datetime64("2026-07-16T14:00:00"),
            np.datetime64("2026-07-16T20:00:00"),  # second timestamp NOT advertised
        ],
    }
    monkeypatch.setattr(coverage_module, "get_store", lambda url: ds)
    monkeypatch.setattr(
        coverage_module.store_registry, "date_index", lambda url: date_index
    )
    product = _product()
    PRODUCTS[product.id] = product
    yield ds
    PRODUCTS.pop(product.id, None)


def test_unknown_collection_returns_none(seeded):
    assert build_coverage_discovery("00000000-0000-0000-0000-000000000000") is None


def test_discovery_shape_and_geometry(seeded):
    doc = build_coverage_discovery(_COLLECTION)
    assert doc["collectionId"] == _COLLECTION
    assert doc["cacheVersion"]
    (entry,) = doc["products"]
    assert entry["id"] == "cov1"
    assert entry["title"] == "Test coverage product"
    assert entry["datasetKey"] == "test_dataset"
    assert entry["default"] is True
    assert entry["variables"] == ["VAR"]
    assert entry["encoding"] == "scalar-rgb24"
    assert entry["maskChannel"] == "A"
    assert entry["tileMatrixSetId"] == "tms_test"
    assert entry["chunkPx"] == [8, 8]
    assert entry["storedPx"] == [10, 10]
    assert entry["padding"] == 1

    lod1 = entry["lods"]["1"]
    assert lod1["grid"] == [2, 2]
    assert lod1["cellSize"] > 0
    gb = lod1["gridBounds"]
    # NW-anchored: grid west/north edges sit half a native step outside the data
    # centres; the far edges cover the data on both axes.
    assert gb["lonMin"] == pytest.approx(96.0 - 1.0, abs=1e-6)
    assert gb["latMax"] == pytest.approx(-8.0 + 1.05263, abs=1e-3)
    assert gb["lonMax"] >= 174.0
    assert gb["latMin"] <= -48.0


def test_discovery_times_use_first_timestamp_per_date(seeded):
    doc = build_coverage_discovery(_COLLECTION)
    (entry,) = doc["products"]
    assert entry["times"] == [
        {"datetime": "2026-07-16T00:00:00+10:00", "dateKey": "2026-07-16"},
        {"datetime": "2026-07-17T00:00:00+10:00", "dateKey": "2026-07-17"},
    ]


def test_discovery_times_respect_date_filters(seeded):
    doc = build_coverage_discovery(_COLLECTION, from_date="2026-07-17")
    (entry,) = doc["products"]
    assert [t["dateKey"] for t in entry["times"]] == ["2026-07-17"]

    doc = build_coverage_discovery(_COLLECTION, to_date="2026-07-16")
    (entry,) = doc["products"]
    assert [t["dateKey"] for t in entry["times"]] == ["2026-07-16"]


def test_discovery_never_leaks_source_paths(seeded):
    doc = build_coverage_discovery(_COLLECTION)
    blob = json.dumps(doc)
    assert "s3://" not in blob
    assert "source_path" not in blob
    assert _SOURCE.split("//")[1].split("/")[0] not in blob  # bucket name


def test_discovery_categorical_includes_flags(monkeypatch):
    ds = _make_store(categorical=True)
    monkeypatch.setattr(coverage_module, "get_store", lambda url: ds)
    monkeypatch.setattr(coverage_module.store_registry, "date_index", lambda url: {})
    product = _product("cov_cat")
    PRODUCTS[product.id] = product
    try:
        doc = build_coverage_discovery(_COLLECTION)
        (entry,) = doc["products"]
        assert entry["flagValues"] == [0, 1, 2]
        assert entry["flagMeanings"] == ["none", "weak", "strong"]
    finally:
        PRODUCTS.pop(product.id, None)


def test_discovery_vector_encoding(monkeypatch):
    ds = _make_store()
    ds["VAR2"] = ds["VAR"]
    monkeypatch.setattr(coverage_module, "get_store", lambda url: ds)
    monkeypatch.setattr(coverage_module.store_registry, "date_index", lambda url: {})
    product = _product("cov_uv", variable=["VAR", "VAR2"])
    PRODUCTS[product.id] = product
    try:
        doc = build_coverage_discovery(_COLLECTION)
        (entry,) = doc["products"]
        assert entry["encoding"] == "vector-rg8"
        assert entry["maskChannel"] == "B"
        assert entry["variables"] == ["VAR", "VAR2"]  # configured order, not caller's
        assert "flagValues" not in entry
    finally:
        PRODUCTS.pop(product.id, None)
