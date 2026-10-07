"""Data-tile manifest routes against the canned tiler sample in LocalStack.

The store sidecar and parquet files in ``tests/canned/s3_tiler_sample1`` are
uploaded under the configured tiler prefix. Dates, bounds, and LOD grids come
from that ``metadata.json``. The conftest patches that stand in for the store
registry are turned off in this module.
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_parquet_types import RootMetadata
from data_access_service.tiler.services.product.catalog import build_catalog
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS, load_products
from data_access_service.tiler.services.store.registry import (
    load_stores,
    store_registry,
)
from data_access_service.tiler.services.store.tiler_repository import close_client
from tests.core.test_with_s3 import REGION, TestWithS3

CANNED = Path(__file__).resolve().parents[2] / "canned" / "s3_tiler_sample1"
STORE = "model_sea_level_anomaly_gridded_delayed"
GSL = f"{STORE}:gsl"
GSLA = f"{STORE}:gsla"
CURRENTS = f"{STORE}:ucur+vcur"
DATE = "1993-01-01T00:00:00Z"
MANIFEST = "/api/v1/das/tiler/data_tiles/{product}/manifest.json?date={date}"


def _canned_metadata() -> dict:
    return json.loads((CANNED / STORE / "metadata.json").read_text())


@pytest.fixture(autouse=True)
def seed_products():
    """This module publishes the canned catalogue itself."""
    yield


@pytest.fixture(autouse=True)
def resolve_timestamp_mock():
    """Use the store registry's time index from the uploaded sidecar."""
    yield None


@pytest.fixture(autouse=True)
def store_available_mock():
    """Use the store registry's loaded-store set."""
    yield


class TestDataTilesWithS3(TestWithS3):
    @pytest.fixture(scope="class", autouse=True)
    def duckdb_on_localstack(self, localstack):
        """Point the tiler DuckDB client at LocalStack. IntTestConfig skips
        the real credential-chain secret."""
        endpoint = localstack.get_url().removeprefix("https://").removeprefix("http://")

        def _local_secret(client: TilerDuckDBClient, bucket: str) -> None:
            client.create_s3_secret_with_keys(
                bucket,
                "test",
                "test",
                endpoint=endpoint,
                use_ssl=False,
                region=REGION,
            )

        patcher = patch.object(TilerDuckDBClient, "create_s3_secret", _local_secret)
        patcher.start()
        close_client()
        yield
        close_client()
        patcher.stop()

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)
        bucket = config.get_datavis_data_bucket_name()
        s3_client.create_bucket(Bucket=bucket)

        # upload_to_s3 keys files relative to the folder it is given, and the
        # tiler reads s3://{datavis}/{root_prefix}/...
        prefix = config.get_tiler_root_dir().removeprefix(f"s3://{bucket}/")
        stage = Path(tempfile.mkdtemp())
        try:
            dest = stage / prefix
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(CANNED, dest)
            TestWithS3.upload_to_s3(s3_client, bucket, stage)
        finally:
            shutil.rmtree(stage)

        root = RootMetadata.from_dict(
            json.loads((CANNED / "root_metadata.json").read_text())
        )
        products = build_catalog({p.id: p for p in root.products if p.store == STORE})
        previous = dict(PRODUCTS)
        load_stores([STORE])
        load_products(products)
        yield
        if previous:
            load_products(previous)
        else:
            PRODUCTS.clear()
        store_registry.clear()
        TestWithS3.delete_object_in_s3(s3_client, bucket)

    def test_manifest_unknown_product(self, client, upload_test_case_to_s3):
        response = client.get(MANIFEST.format(product="nonexistent", date=DATE))
        assert response.status_code == 404

    def test_manifest_missing_date(self, client, upload_test_case_to_s3):
        response = client.get(
            MANIFEST.format(product=GSLA, date="9999-01-01T00:00:00Z")
        )
        assert response.status_code == 404

    def test_manifest_missing_store(self, client, upload_test_case_to_s3):
        """A registered product whose sidecar was never loaded 404s before
        the slice is opened."""
        PRODUCTS["missing_store_product"] = Product(
            id="missing_store_product", store="no_such_store", variable="GSLA"
        )
        response = client.get(
            MANIFEST.format(product="missing_store_product", date=DATE)
        )
        assert response.status_code == 404
        assert "failed to open" in response.json()["detail"]

    def test_manifest_cancelled_client_disconnect_short_circuits_to_499(
        self, client, upload_test_case_to_s3
    ):
        with (
            patch("data_access_service.core.tiler_routes.shared.load_slice") as load,
            patch(
                "starlette.requests.Request.is_disconnected",
                new=AsyncMock(return_value=True),
            ),
        ):
            response = client.get(MANIFEST.format(product=GSLA, date=DATE))
            load.assert_not_called()
        assert response.status_code == 499

    def test_manifest_ok(self, client, upload_test_case_to_s3):
        meta = _canned_metadata()
        response = client.get(MANIFEST.format(product=GSLA, date=DATE))
        assert response.status_code == 200
        body = response.json()
        assert body["bounds"] == {
            "lonMin": min(meta["lon"]),
            "lonMax": max(meta["lon"]),
            "latMin": min(meta["lat"]),
            "latMax": max(meta["lat"]),
        }
        assert body["valueRange"] == pytest.approx(
            [-0.7246730923652649, 0.5397281050682068]
        )
        assert body["lods"]
        assert "flagValues" not in body

    def test_manifest_vector_ranges(self, client, upload_test_case_to_s3):
        response = client.get(MANIFEST.format(product=CURRENTS, date=DATE))
        assert response.status_code == 200
        body = response.json()
        assert body["uRange"] == pytest.approx([-1.2680634260177612, 1.152854323387146])
        assert body["vRange"] == pytest.approx(
            [-1.8807318210601807, 1.5303740501403809]
        )
        assert "valueRange" not in body

    def test_gsl_date_range_extends_past_the_other_variables(
        self, client, upload_test_case_to_s3
    ):
        """GSL lists one extra instant, so its range is wider than GSLA."""
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
        assert response.status_code == 200
        products = response.json()["products"]
        assert products[GSL]["full_date_range"] == {
            "start": "1993-01-01T00:00:00Z",
            "end": "1993-01-02T00:00:00Z",
        }
        assert products[GSL]["available_dates"] == [
            "1993-01-01T00:00:00Z",
            "1993-01-02T00:00:00Z",
        ]
        assert products[GSLA]["full_date_range"] == {
            "start": "1993-01-01T00:00:00Z",
            "end": "1993-01-01T00:00:00Z",
        }
        assert products[CURRENTS]["full_date_range"] == {
            "start": "1993-01-01T00:00:00Z",
            "end": "1993-01-01T00:00:00Z",
        }
