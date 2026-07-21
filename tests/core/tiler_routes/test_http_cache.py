"""Exercises the actual caching contract from config/tiler/http_cache.py end to end."""

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


# --- REVALIDATE endpoints: ETag round-trips to 304 -------------------------


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
        first = client.get("/api/v1/das/tiler/data_tiles/manifest")
        assert first.status_code == 200
        assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
        etag = first.headers["etag"]

        second = client.get(
            "/api/v1/das/tiler/data_tiles/manifest",
            headers={"if-none-match": etag},
        )
    assert second.status_code == 304
    assert second.content == b""


def test_products_etag_round_trips_to_304(client):
    first = client.get("/api/v1/das/tiler/data_tiles/products")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
    etag = first.headers["etag"]

    second = client.get(
        "/api/v1/das/tiler/data_tiles/products",
        headers={"if-none-match": etag},
    )
    assert second.status_code == 304


def test_colormaps_etag_round_trips_to_304(client):
    first = client.get("/api/v1/das/tiler/visual_tiles/colormaps")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
    etag = first.headers["etag"]

    second = client.get(
        "/api/v1/das/tiler/visual_tiles/colormaps",
        headers={"if-none-match": etag},
    )
    assert second.status_code == 304
