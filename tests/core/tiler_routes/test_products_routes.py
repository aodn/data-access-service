"""Products / manifest / point endpoints (shared router, both prefixes)."""

from unittest.mock import patch

import pytest

from .conftest import (
    NON_VISUAL_PRODUCT_ID,
    SEEDED_DATE_ISO,
    TEST_UUID,
    UUID_PRODUCT_ID,
    VISUAL_PRODUCT_ID,
)

# The same products/manifest/point router is mounted under both prefixes.
PREFIXES = ["data_tiles", "visual_tiles"]


def _base(prefix: str) -> str:
    return f"/api/v1/das/tiler/{prefix}"


# --- GET /products -----------------------------------------------------------


@pytest.mark.parametrize("prefix", PREFIXES)
def test_products_lists_seeded_products(client, prefix):
    r = client.get(f"{_base(prefix)}/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    assert VISUAL_PRODUCT_ID in by_id
    assert NON_VISUAL_PRODUCT_ID in by_id
    # ProductConfig fields, not the old data_tile/coastal_fill schema.
    sla = by_id[VISUAL_PRODUCT_ID]
    assert sla["variable"] == "GSLA"
    assert sla["visual"] is True
    assert "data_tile" in sla  # default DataTileConfig, still present
    assert by_id[NON_VISUAL_PRODUCT_ID]["visual"] is False


@pytest.mark.parametrize("prefix", PREFIXES)
def test_products_metadata_uuid_serialized(client, prefix):
    r = client.get(f"{_base(prefix)}/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    assert by_id[UUID_PRODUCT_ID]["metadata_uuid"] == TEST_UUID


# --- GET /manifest -----------------------------------------------------------


@pytest.mark.parametrize("prefix", PREFIXES)
def test_manifest_all_products(client, prefix):
    r = client.get(f"{_base(prefix)}/manifest")
    assert r.status_code == 200
    products = r.json()["products"]
    assert VISUAL_PRODUCT_ID in products
    entry = products[VISUAL_PRODUCT_ID]
    assert entry["available_dates"] == [SEEDED_DATE_ISO]
    assert entry["full_date_range"] == {
        "start": SEEDED_DATE_ISO,
        "end": SEEDED_DATE_ISO,
    }


@pytest.mark.parametrize("prefix", PREFIXES)
def test_manifest_from_to_filtering(client, prefix):
    dates = ["2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z", "2024-12-01T00:00:00Z"]
    with patch(
        "data_access_service.core.tiler_routes.products.available_dates_iso",
        return_value=dates,
    ):
        r = client.get(
            f"{_base(prefix)}/manifest"
            "?from=2024-05-01T00:00:00Z&to=2024-07-01T00:00:00Z"
        )
    assert r.status_code == 200
    entry = r.json()["products"][VISUAL_PRODUCT_ID]
    assert entry["available_dates"] == ["2024-06-01T00:00:00Z"]
    # full_date_range spans the whole dataset, not the filtered subset.
    assert entry["full_date_range"] == {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-12-01T00:00:00Z",
    }


@pytest.mark.parametrize("prefix", PREFIXES)
def test_manifest_empty_in_range(client, prefix):
    with patch(
        "data_access_service.core.tiler_routes.products.available_dates_iso",
        return_value=["2020-01-01T00:00:00Z"],
    ):
        r = client.get(f"{_base(prefix)}/manifest?to=2019-01-01T00:00:00Z")
    assert r.status_code == 200
    entry = r.json()["products"][VISUAL_PRODUCT_ID]
    assert entry["available_dates"] == []
    assert entry["full_date_range"] == {
        "start": "2020-01-01T00:00:00Z",
        "end": "2020-01-01T00:00:00Z",
    }


@pytest.mark.parametrize("prefix", PREFIXES)
def test_manifest_metadata_uuid_filters(client, prefix):
    r = client.get(f"{_base(prefix)}/manifest?metadata_uuid={TEST_UUID}")
    assert r.status_code == 200
    products = r.json()["products"]
    assert UUID_PRODUCT_ID in products
    assert VISUAL_PRODUCT_ID not in products


@pytest.mark.parametrize("prefix", PREFIXES)
def test_manifest_metadata_uuid_no_match_is_404(client, prefix):
    r = client.get(f"{_base(prefix)}/manifest?metadata_uuid=does-not-exist")
    assert r.status_code == 404
    assert "does-not-exist" in r.json()["detail"]


# --- GET /{product_id}/point -------------------------------------------------


@pytest.mark.parametrize("prefix", PREFIXES)
def test_point_ok(client, prefix):
    with patch(
        "data_access_service.core.tiler_routes.products.lookup_point",
        return_value=0.42,
    ):
        r = client.get(
            f"{_base(prefix)}/{VISUAL_PRODUCT_ID}/point"
            f"?date={SEEDED_DATE_ISO}&lat=-35&lon=145"
        )
    assert r.status_code == 200
    body = r.json()
    assert body["lat"] == -35
    assert body["lon"] == 145
    assert body["values"] == [{"variable": "GSLA", "value": 0.42}]


@pytest.mark.parametrize("prefix", PREFIXES)
def test_point_unknown_product_404(client, prefix):
    r = client.get(
        f"{_base(prefix)}/nonexistent/point?date={SEEDED_DATE_ISO}&lat=-35&lon=145"
    )
    assert r.status_code == 404


@pytest.mark.parametrize("prefix", PREFIXES)
def test_point_bad_date_422(client, prefix):
    r = client.get(
        f"{_base(prefix)}/{VISUAL_PRODUCT_ID}/point?date=not-a-date&lat=-35&lon=145"
    )
    assert r.status_code == 422


@pytest.mark.parametrize("prefix", PREFIXES)
def test_point_unavailable_date_404(client, prefix):
    r = client.get(
        f"{_base(prefix)}/{VISUAL_PRODUCT_ID}/point"
        "?date=1999-01-01T00:00:00Z&lat=-35&lon=145"
    )
    assert r.status_code == 404
