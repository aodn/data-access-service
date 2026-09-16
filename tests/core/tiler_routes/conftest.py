"""Test harness for the parquet-backed tiler routes.

The tiler was migrated off the old services/store architecture onto a slim
parquet-backed design (data_access_service/tiler/{catalog,product,render}).
These fixtures boot the real FastAPI app, mark the tiler ready, and seed the
in-process product registry (data_access_service.tiler.product.PRODUCTS) with
representative Product instances so route tests never touch S3 or DuckDB.
"""

from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.tiler_routes.shared import mark_tiler_ready
from data_access_service.server import app
from data_access_service.tiler.product import Product, load_products

# Product.timestamps stores the compact "%Y%m%dT%H%M%SZ" form; the route
# helper resolve_timestamp_or_404 accepts the compact or ISO variant, and
# catalog.available_dates_iso re-expands them to ISO Z for the manifest.
SEEDED_DATE_ISO = "2024-02-24T00:00:00Z"
SEEDED_DATE_COMPACT = "20240224T000000Z"

VISUAL_PRODUCT_ID = "sea_level_anomaly"
NON_VISUAL_PRODUCT_ID = "wind_direction"
UUID_PRODUCT_ID = "heatwave:sst"

TEST_UUID = "uuid-abc-123"


def _make_products() -> dict[str, Product]:
    visual = Product(
        id=VISUAL_PRODUCT_ID,
        uuid="uuid-sla",
        dataset="sea_level_anomaly.zarr",
        variable="GSLA",
        timestamps=(SEEDED_DATE_COMPACT,),
        n_i=8,
        n_j=8,
        lat_min=-40.0,
        lat_max=-30.0,
        lon_min=140.0,
        lon_max=150.0,
        vmin=-1.0,
        vmax=1.0,
        source_path="s3://test-bucket/sea_level_anomaly.parquet",
        visual=True,
    )
    non_visual = Product(
        id=NON_VISUAL_PRODUCT_ID,
        uuid="uuid-wdir",
        dataset="wind.zarr",
        variable="WDIR",
        timestamps=(SEEDED_DATE_COMPACT,),
        n_i=8,
        n_j=8,
        lat_min=-40.0,
        lat_max=-30.0,
        lon_min=140.0,
        lon_max=150.0,
        source_path="s3://test-bucket/wind.parquet",
        visual=False,
    )
    with_uuid = Product(
        id=UUID_PRODUCT_ID,
        uuid=TEST_UUID,
        dataset="heatwave.zarr",
        variable="sst",
        timestamps=(SEEDED_DATE_COMPACT,),
        n_i=8,
        n_j=8,
        lat_min=-40.0,
        lat_max=-30.0,
        lon_min=140.0,
        lon_max=150.0,
        source_path="s3://test-bucket/heatwave.parquet",
        visual=True,
    )
    return {p.id: p for p in (visual, non_visual, with_uuid)}


@pytest.fixture(autouse=True)
def seed_products():
    """Populate the product registry before each test; restore it after."""
    from data_access_service.tiler import product as product_module

    original = dict(product_module._PRODUCTS)
    load_products(_make_products())
    yield product_module._PRODUCTS
    load_products(original)


@pytest.fixture
def client():
    """Boot the real app via TestClient.

    Entering TestClient as a context manager triggers the lifespan, so mock
    API to avoid a real (slow, network-bound) metadata init, mark the tiler
    ready, and override the api-key dependency for auth.
    """
    mock_instance = MagicMock()
    mock_instance.get_api_status.return_value = True
    mark_tiler_ready()
    app.dependency_overrides[api_key_auth] = lambda: "testing"
    with patch("data_access_service.server.API", return_value=mock_instance):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c
    del app.dependency_overrides[api_key_auth]
