from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.server import app
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS
from data_access_service.core.routes.auth import api_key_auth


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


@pytest.fixture(autouse=True)
def resolve_timestamp_mock():
    """shared.resolve_timestamp_or_404 checks the real store registry, which
    would otherwise try to open a real (non-existent, in tests) S3 store.
    Route tests only mock load_slice, so default this to "date resolves";
    a test simulating a missing date overrides the return value to None.
    """
    with patch(
        "data_access_service.core.tiler_routes.shared.resolve_timestamp",
        return_value="raw-ts",
    ) as m:
        yield m


@pytest.fixture
def client():
    """Entering TestClient as a context manager triggers lifespan / api_setup.
    Mock API so that does not also trigger a real (slow, network-bound)
    API.initialize_metadata() call. Routes are registered once at import time.
    """
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    mark_tiler_ready()
    app.dependency_overrides[api_key_auth] = lambda: "testing"
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c
    del app.dependency_overrides[api_key_auth]
