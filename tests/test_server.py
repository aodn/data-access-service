from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.server import app
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS

# A PNG large enough to clear GZipMiddleware's minimum_size, so the no-gzip assertion
# below exercises the content-type exclusion rather than passing on the size threshold.
_BIG_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 4000


@pytest.fixture(autouse=True)
def seed_products():
    """test_gzip_skips_image_tiles hits a real product route; seed the one it needs."""
    PRODUCTS["sea_level_anomaly"] = Product(
        id="sea_level_anomaly", source_path="s3://test/sla.zarr", variable="GSLA"
    )
    yield
    PRODUCTS.pop("sea_level_anomaly", None)


@pytest.fixture
def client():
    """Entering TestClient as a context manager triggers lifespan, which is what
    registers routes via api_setup() — mock API out so that doesn't also trigger
    a real (slow, network-bound) API.initialize_metadata() call."""
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c


def test_openapi_schema(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert "paths" in response.json()


def test_gzip_compresses_json(client):
    # /openapi.json is well over minimum_size, so a gzip-capable client must get it compressed.
    response = client.get("/openapi.json", headers={"Accept-Encoding": "gzip"})
    assert response.status_code == 200
    assert response.headers.get("content-encoding") == "gzip"


def test_gzip_skips_image_tiles(client):
    # PNG tiles are already compressed; gzipping them wastes CPU. The image/* deny-list
    # entry must keep them uncompressed even though the body exceeds minimum_size and the
    # client advertises gzip. This guards the Starlette-global monkeypatch in main.py.
    with (
        patch("data_access_service.core.tiler_routes.shared.load_slice"),
        patch(
            "data_access_service.core.tiler_routes.visual_tiles.render_tile",
            return_value=_BIG_PNG,
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/visual_tiles/sea_level_anomaly/2024-01-01/5/0/0.png",
            headers={"Accept-Encoding": "gzip"},
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.headers.get("content-encoding") != "gzip"
