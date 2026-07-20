from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.config.tiler.http_cache import require_cache_version
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.server import app
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS
from data_access_service.utils.api_utils import api_key_auth


@pytest.fixture(autouse=True)
def seed_products():
    """Populate PRODUCTS with test fixtures before each test and clean up after."""
    test_products = [
        Product(
            id="sea_level_anomaly", source_path="s3://test/sla.zarr", variable="GSLA"
        ),
        Product(
            id="ocean_current",
            source_path="s3://test/sla.zarr",
            variable=["UCUR", "VCUR"],
        ),
    ]
    for p in test_products:
        PRODUCTS[p.id] = p
    yield
    for p in test_products:
        PRODUCTS.pop(p.id, None)


@pytest.fixture
def client():
    """Entering TestClient as a context manager triggers lifespan, which is what
    registers routes via api_setup() — mock API out so that doesn't also trigger
    a real (slow, network-bound) API.initialize_metadata() call.

    Overrides require_cache_version so existing tests don't need to append
    ?cv=... to every immutable-endpoint URL — that enforcement is exercised
    directly in test_http_cache.py with a client that doesn't override it.
    """
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    mark_tiler_ready()
    app.dependency_overrides[api_key_auth] = lambda: "testing"
    app.dependency_overrides[require_cache_version] = lambda: None
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c
    del app.dependency_overrides[api_key_auth]
    del app.dependency_overrides[require_cache_version]
