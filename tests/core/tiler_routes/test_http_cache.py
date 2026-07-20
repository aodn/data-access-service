"""Exercises the actual caching contract from config/tiler/http_cache.py end to end.

The shared `client` fixture (conftest.py) overrides require_cache_version so the
rest of the suite doesn't need to append ?cv=... everywhere. This file uses its
own client, built the same way but *without* that override, so enforcement is
tested for real.
"""

from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.server import app
from data_access_service.utils.api_utils import api_key_auth

from .test_data_tiles import _make_ds

_FAKE_PRODUCTS = {}


@pytest.fixture
def strict_client():
    """Same as conftest's `client`, minus the require_cache_version override."""
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    mark_tiler_ready()
    app.dependency_overrides[api_key_auth] = lambda: "testing"
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c
    del app.dependency_overrides[api_key_auth]


# --- require_cache_version enforcement on immutable endpoints -------------

_POINT_URL = "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/point?lat=-35&lon=145"


def test_immutable_endpoint_rejects_missing_cv(strict_client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = strict_client.get(_POINT_URL)
    assert response.status_code == 400
    assert "/manifest" in response.json()["detail"]


def test_immutable_endpoint_rejects_stale_cv(strict_client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = strict_client.get(_POINT_URL + "&cv=not-the-current-version")
    assert response.status_code == 400


def test_immutable_endpoint_accepts_current_cv(strict_client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = strict_client.get(_POINT_URL + "&cv=cv1")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=31536000, immutable"


# --- REVALIDATE endpoints: no cv required, ETag round-trips to 304 --------


def test_manifest_is_revalidate_and_not_gated_on_cv(strict_client):
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
        first = strict_client.get("/api/v1/das/tiler/data_tiles/manifest")
        assert first.status_code == 200
        assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
        etag = first.headers["etag"]

        second = strict_client.get(
            "/api/v1/das/tiler/data_tiles/manifest",
            headers={"if-none-match": etag},
        )
    assert second.status_code == 304
    assert second.content == b""


def test_products_etag_round_trips_to_304(strict_client):
    first = strict_client.get("/api/v1/das/tiler/data_tiles/products")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
    etag = first.headers["etag"]

    second = strict_client.get(
        "/api/v1/das/tiler/data_tiles/products",
        headers={"if-none-match": etag},
    )
    assert second.status_code == 304


def test_colormaps_etag_round_trips_to_304(strict_client):
    first = strict_client.get("/api/v1/das/tiler/visual_tiles/colormaps")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=300, must-revalidate"
    etag = first.headers["etag"]

    second = strict_client.get(
        "/api/v1/das/tiler/visual_tiles/colormaps",
        headers={"if-none-match": etag},
    )
    assert second.status_code == 304
