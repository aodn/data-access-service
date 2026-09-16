"""Smoke test: the parquet-backed tiler harness boots and serves products."""

from .conftest import VISUAL_PRODUCT_ID


def test_products_smoke(client):
    r = client.get("/api/v1/das/tiler/data_tiles/products")
    assert r.status_code == 200
    ids = {p["id"] for p in r.json()}
    assert VISUAL_PRODUCT_ID in ids
