"""The availability manifest is per-store and fails one store at a time.

ogcapi-java fetches this global manifest on *every* getCollectionProducts call.
Before isolation, one unreachable store made the route raise, which broke the
product listing for every collection — a global outage wearing the costume of a
local degradation. At two stores that risk was theoretical; at sixty it is not.
"""

import pytest

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import Product

MANIFEST = "/api/v1/das/tiler/data_tiles/manifest"

STORE_A = "s3://test/a.zarr"
STORE_B = "s3://test/b.zarr"


@pytest.fixture
def catalogue(monkeypatch):
    """Exactly three products over two stores — two of them sharing store A.

    Replaces the whole registry rather than adding to it, so the store-count
    assertions below are not perturbed by the conftest's seed products.
    """
    monkeypatch.setattr(
        registry,
        "PRODUCTS",
        {
            product.id: product
            for product in (
                Product(id="a:one", source_path=STORE_A, variable="V1"),
                Product(id="a:two", source_path=STORE_A, variable="V2"),
                Product(id="b:one", source_path=STORE_B, variable="V1"),
            )
        },
    )


def _patch_dates(monkeypatch, behaviour):
    """Back get_available_dates with a per-store dict of dates or exceptions."""
    calls: list[str] = []

    def fake(store_url):
        calls.append(store_url)
        result = behaviour[store_url]
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(
        "data_access_service.core.tiler_routes.products.get_available_dates", fake
    )
    return calls


def test_dates_are_resolved_once_per_unique_store(client, catalogue, monkeypatch):
    """Availability is a property of the store, not the product. Two products
    sharing a store must not cost two lookups — each one opens the store."""
    calls = _patch_dates(
        monkeypatch, {STORE_A: ["2024-01-01", "2024-01-02"], STORE_B: ["2024-02-01"]}
    )

    response = client.get(MANIFEST)

    assert response.status_code == 200
    assert sorted(calls) == [STORE_A, STORE_B]
    assert len(calls) == 2, "three products but only two stores"


def test_products_sharing_a_store_report_the_same_availability(
    client, catalogue, monkeypatch
):
    _patch_dates(monkeypatch, {STORE_A: ["2024-01-01"], STORE_B: ["2024-02-01"]})

    products = client.get(MANIFEST).json()["products"]

    assert products["a:one"]["available_dates"] == ["2024-01-01"]
    assert products["a:two"]["available_dates"] == ["2024-01-01"]
    assert products["b:one"]["available_dates"] == ["2024-02-01"]


def test_one_failing_store_still_yields_200(client, catalogue, monkeypatch):
    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )

    response = client.get(MANIFEST)

    assert response.status_code == 200


def test_only_the_failing_stores_products_are_degraded(client, catalogue, monkeypatch):
    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )

    products = client.get(MANIFEST).json()["products"]

    for degraded in ("a:one", "a:two"):
        assert products[degraded]["available_dates"] == []
        assert products[degraded]["full_date_range"] == {"start": None, "end": None}

    # The healthy store is completely unaffected.
    assert products["b:one"]["available_dates"] == ["2024-02-01"]
    assert products["b:one"]["full_date_range"] == {
        "start": "2024-02-01",
        "end": "2024-02-01",
    }


def test_every_product_is_still_listed_when_a_store_fails(
    client, catalogue, monkeypatch
):
    """A degraded product stays visible with empty availability rather than
    disappearing from the catalogue."""
    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )

    products = client.get(MANIFEST).json()["products"]

    assert set(products) >= {"a:one", "a:two", "b:one"}


def test_route_fails_only_when_no_store_resolves(client, catalogue, monkeypatch):
    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: RuntimeError("also down")},
    )

    response = client.get(MANIFEST)

    assert response.status_code == 503


def test_date_filters_still_apply_to_healthy_stores(client, catalogue, monkeypatch):
    _patch_dates(
        monkeypatch,
        {
            STORE_A: RuntimeError("s3 unreachable"),
            STORE_B: ["2024-01-01", "2024-02-01", "2024-03-01"],
        },
    )

    products = client.get(f"{MANIFEST}?from=2024-02-01").json()["products"]

    assert products["b:one"]["available_dates"] == ["2024-02-01", "2024-03-01"]
    # full_date_range stays the store's full bounds, independent of from/to.
    assert products["b:one"]["full_date_range"]["start"] == "2024-01-01"


def test_failure_is_logged_once_per_failing_store(
    client, catalogue, monkeypatch, caplog
):
    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )

    with caplog.at_level("WARNING"):
        client.get(MANIFEST)

    summaries = [
        r for r in caplog.records if "failed to resolve availability" in r.message
    ]
    assert len(summaries) == 1
    assert "1 of 2 stores" in summaries[0].getMessage()
