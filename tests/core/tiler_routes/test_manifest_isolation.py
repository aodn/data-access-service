"""The availability manifest is per-store and fails one store at a time.

ogcapi-java fetches this global manifest on every getCollectionProducts call,
so one unreachable store must not break the listing for every collection.
"""

import pandas as pd
import pytest

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import Product

MANIFEST_BASE = "/api/v1/das/tiler/data_tiles/manifest"
# from=2000-01-01 bypasses the "start of last year" default so these tests'
# fixture dates (all 2024) aren't silently filtered out of available_dates —
# this file is about per-store isolation, not the from/to default.
MANIFEST = f"{MANIFEST_BASE}?from=2000-01-01T00:00:00Z"

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
    """Back get_available_dates with a per-store dict of date strings or exceptions.

    Wraps each date string into the (iso_string, timestamp) shape
    get_available_dates itself returns, so test bodies can stay in plain strings.
    """
    calls: list[str] = []

    def fake(store_url):
        calls.append(store_url)
        result = behaviour[store_url]
        if isinstance(result, BaseException):
            raise result
        return [(d, pd.Timestamp(d)) for d in result]

    monkeypatch.setattr(
        "data_access_service.core.tiler_routes.products.get_available_dates", fake
    )
    return calls


def test_dates_are_resolved_once_per_unique_store(client, catalogue, monkeypatch):
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


def test_total_failure_of_absent_stores_is_a_404_naming_them(
    client, catalogue, monkeypatch
):
    """A store that is *absent* is a different answer from one that could not be
    reached. When every store is absent the app's FileNotFoundError handler
    answers 404 with the path, rather than a generic "try again later"."""
    _patch_dates(
        monkeypatch,
        {
            STORE_A: FileNotFoundError(f"No such file or directory: '{STORE_A}'"),
            STORE_B: FileNotFoundError(f"No such file or directory: '{STORE_B}'"),
        },
    )

    response = client.get(MANIFEST)

    assert response.status_code == 404
    assert STORE_A in response.json()["detail"]


def test_mixed_total_failure_prefers_the_retryable_answer(
    client, catalogue, monkeypatch
):
    _patch_dates(
        monkeypatch,
        {
            STORE_A: FileNotFoundError(f"No such file or directory: '{STORE_A}'"),
            STORE_B: RuntimeError("s3 unreachable"),
        },
    )

    assert client.get(MANIFEST).status_code == 503


def test_one_absent_store_among_healthy_ones_still_yields_200(
    client, catalogue, monkeypatch
):
    _patch_dates(
        monkeypatch,
        {
            STORE_A: FileNotFoundError(f"No such file or directory: '{STORE_A}'"),
            STORE_B: ["2024-02-01"],
        },
    )

    response = client.get(MANIFEST)

    assert response.status_code == 200
    assert response.json()["products"]["a:one"]["available_dates"] == []
    assert response.json()["products"]["b:one"]["available_dates"] == ["2024-02-01"]


def test_date_filters_still_apply_to_healthy_stores(client, catalogue, monkeypatch):
    _patch_dates(
        monkeypatch,
        {
            STORE_A: RuntimeError("s3 unreachable"),
            STORE_B: ["2024-01-01", "2024-02-01", "2024-03-01"],
        },
    )

    products = client.get(f"{MANIFEST_BASE}?from=2024-02-01T00:00:00Z").json()[
        "products"
    ]

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


# --- failure cooldown -------------------------------------------------------


def test_a_failed_store_is_not_re_attempted_on_every_request(
    client, catalogue, monkeypatch
):
    """StoreRegistry does not cache a failed open, so without a cooldown every
    /manifest call would pay the full S3 timeout for every dead store — and
    ogcapi-java calls this route on every getCollectionProducts."""
    calls = _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )

    for _ in range(5):
        assert client.get(MANIFEST).status_code == 200

    assert calls.count(STORE_A) == 1, "the dead store was re-attempted"
    assert calls.count(STORE_B) == 5, "healthy stores are still read every time"


def test_cooldown_replays_the_same_error_so_the_status_stays_stable(
    client, catalogue, monkeypatch
):
    """An absent-store total failure must keep answering 404, not flip to 503
    once the cooldown starts short-circuiting the lookup."""
    _patch_dates(
        monkeypatch,
        {
            STORE_A: FileNotFoundError(f"No such file or directory: '{STORE_A}'"),
            STORE_B: FileNotFoundError(f"No such file or directory: '{STORE_B}'"),
        },
    )

    first = client.get(MANIFEST)
    second = client.get(MANIFEST)

    assert first.status_code == 404
    assert second.status_code == 404


def test_cooldown_clears_once_the_store_recovers(client, catalogue, monkeypatch):
    from data_access_service.core.tiler_routes import products

    _patch_dates(
        monkeypatch,
        {STORE_A: RuntimeError("s3 unreachable"), STORE_B: ["2024-02-01"]},
    )
    client.get(MANIFEST)
    assert STORE_A in products._recent_store_failures

    monkeypatch.setattr(products, "_STORE_FAILURE_COOLDOWN_SECONDS", 0.0)
    _patch_dates(monkeypatch, {STORE_A: ["2024-03-01"], STORE_B: ["2024-02-01"]})

    body = client.get(MANIFEST).json()["products"]

    assert body["a:one"]["available_dates"] == ["2024-03-01"]
    assert STORE_A not in products._recent_store_failures
