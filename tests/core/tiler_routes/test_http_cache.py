"""Exercises the actual caching contract from config/http_cache.py end to end."""

from unittest.mock import patch

from .test_data_tiles import _make_ds

_FAKE_PRODUCTS = {}

_POINT_URL = (
    "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/point?lat=-35&lon=145"
)


def test_immutable_endpoint_disables_browser_caching_but_not_cdn(client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = client.get(_POINT_URL)
    assert response.status_code == 200
    assert (
        response.headers["cache-control"]
        == "public, s-maxage=31536000, max-age=0, must-revalidate"
    )


# --- REVALIDATE endpoints: short CDN TTL, no ETag ---------------------------


def test_manifest_is_revalidate(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=[],
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300, must-revalidate"
    assert "etag" not in response.headers


def test_products_is_revalidate(client):
    response = client.get("/api/v1/das/tiler/data_tiles/products")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300, must-revalidate"
    assert "etag" not in response.headers


def test_colormaps_is_revalidate(client):
    response = client.get("/api/v1/das/tiler/visual_tiles/colormaps")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300, must-revalidate"
    assert "etag" not in response.headers
