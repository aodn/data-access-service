from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.server import app
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS


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
    a real (slow, network-bound) API.initialize_metadata() call."""
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c
