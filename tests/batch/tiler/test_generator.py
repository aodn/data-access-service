from unittest.mock import MagicMock

import pytest

from data_access_service.batch.tiler import generator
from data_access_service.batch.tiler.generator import (
    _group_by_store,
    generate_tiler_parquet_for_all_products,
    write_root_metadata,
)
from data_access_service.models.tiler_parquet_types import ProductIdentity
from data_access_service.models.tiler_types import TilerDuckDBConfig, TilerParquetConfig


def _product(
    pid: str, source_path: str, variable, uuid: str = "uuid-a"
) -> ProductIdentity:
    return ProductIdentity(
        id=pid, source_path=source_path, variable=variable, metadata_uuid=uuid
    )


def _tp_config(
    output_dir: str = "s3://my-bucket/tiler", **overrides
) -> TilerParquetConfig:
    base = dict(
        output_dir=output_dir,
        batch_days=30,
        max_timestamps=5,
        duckdb=TilerDuckDBConfig(
            memory_limit="256MB",
            threads=1,
        ),
    )
    base.update(overrides)
    return TilerParquetConfig(**base)


def _fake_s3_json_store(monkeypatch) -> dict[str, dict]:
    """In-memory stand-in for S3 objects, keyed by path — patches
    generator.storage.read_json/write_json so write_root_metadata's upsert
    logic can be tested without real S3."""
    store: dict[str, dict] = {}
    monkeypatch.setattr(generator.storage, "read_json", lambda path: store.get(path))
    monkeypatch.setattr(
        generator.storage,
        "write_json",
        lambda path, data: store.__setitem__(path, data),
    )
    return store


class TestGroupByStore:
    def test_merges_variables_from_multiple_products_on_one_store(self):
        products = {
            "p1": _product("p1", "s3://b/x.zarr", "GSL"),
            "p2": _product("p2", "s3://b/x.zarr", "GSLA"),
            "p3": _product("p3", "s3://b/y.zarr", ["UCUR", "VCUR"], uuid="uuid-b"),
        }
        grouped = _group_by_store(products)
        assert grouped == {
            "s3://b/x.zarr": ("uuid-a", ["GSL", "GSLA"]),
            "s3://b/y.zarr": ("uuid-b", ["UCUR", "VCUR"]),
        }


class TestWriteRootMetadataToS3:
    """No real S3 here — storage.read_json/write_json are mocked, so this
    only checks write_root_metadata's own upsert logic against whatever
    storage.read_json returns."""

    def test_upserts_onto_existing_s3_content(self, monkeypatch):
        existing = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [
                _product("old", "s3://b/old.zarr", "v", uuid="uuid-old").to_dict()
            ],
        }
        monkeypatch.setattr(generator.storage, "read_json", lambda path: existing)
        written = {}
        monkeypatch.setattr(
            generator.storage,
            "write_json",
            lambda path, data: written.update(path=path, data=data),
        )

        products = [_product("new", "s3://b/new.zarr", "v", uuid="uuid-new")]
        path = write_root_metadata(products, "s3://my-bucket/tiler")

        assert path == "s3://my-bucket/tiler/root_metadata.json"
        assert written["path"] == path
        assert sorted(p["id"] for p in written["data"]["products"]) == ["new", "old"]

    def test_writes_fresh_manifest_when_nothing_exists_yet(self, monkeypatch):
        monkeypatch.setattr(generator.storage, "read_json", lambda path: None)
        written = {}
        monkeypatch.setattr(
            generator.storage,
            "write_json",
            lambda path, data: written.update(data=data),
        )

        products = [_product("p1", "s3://b/x.zarr", "v")]
        write_root_metadata(products, "s3://my-bucket/tiler")

        assert [p["id"] for p in written["data"]["products"]] == ["p1"]


@pytest.fixture(autouse=True)
def stub_log_memory(monkeypatch):
    monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)


class TestGenerateForAllProducts:
    def test_skips_stores_that_fail_prewarm(self, monkeypatch):
        s3_store = _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "s3://b/good.zarr", "v"),
            "p2": _product("p2", "s3://b/bad.zarr", "v", uuid="uuid-b"),
        }
        monkeypatch.setattr(
            generator, "discover_products", lambda api, base_url: products
        )

        async def fake_prewarm(urls):
            return {"s3://b/good.zarr": None, "s3://b/bad.zarr": ValueError("boom")}

        monkeypatch.setattr(generator, "prewarm_stores", fake_prewarm)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_config=lambda: MagicMock(co_bucket="s3://bucket"),
                get_tiler_parquet_config=lambda: _tp_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store_url, uuid, variables, tp_config: calls.append(store_url)
            or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        assert calls == ["s3://b/good.zarr"]
        # Only the successfully-converted store's product is published.
        root = s3_store["s3://my-bucket/tiler/root_metadata.json"]
        assert [p["id"] for p in root["products"]] == ["p1"]

    def test_filters_by_uuid(self, monkeypatch):
        _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "s3://b/x.zarr", "v", uuid="uuid-a"),
            "p2": _product("p2", "s3://b/y.zarr", "v", uuid="uuid-b"),
        }
        monkeypatch.setattr(
            generator, "discover_products", lambda api, base_url: products
        )

        async def fake_prewarm(urls):
            return {url: None for url in urls}

        monkeypatch.setattr(generator, "prewarm_stores", fake_prewarm)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_config=lambda: MagicMock(co_bucket="s3://bucket"),
                get_tiler_parquet_config=lambda: _tp_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store_url, uuid, variables, tp_config: calls.append(store_url)
            or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-b")

        assert calls == ["s3://b/y.zarr"]

    def test_root_metadata_upserts_without_dropping_other_uuids(self, monkeypatch):
        """A uuid-scoped run must not wipe out other uuids already published."""
        s3_store = _fake_s3_json_store(monkeypatch)
        s3_store["s3://my-bucket/tiler/root_metadata.json"] = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [
                _product("old", "s3://b/old.zarr", "v", uuid="uuid-old").to_dict()
            ],
        }
        products = {"p1": _product("p1", "s3://b/new.zarr", "v", uuid="uuid-new")}
        monkeypatch.setattr(
            generator, "discover_products", lambda api, base_url: products
        )

        async def fake_prewarm(urls):
            return {url: None for url in urls}

        monkeypatch.setattr(generator, "prewarm_stores", fake_prewarm)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_config=lambda: MagicMock(co_bucket="s3://bucket"),
                get_tiler_parquet_config=lambda: _tp_config(),
            ),
        )
        monkeypatch.setattr(generator, "_build_in_subprocess", lambda *a, **k: True)

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-new")

        root = s3_store["s3://my-bucket/tiler/root_metadata.json"]
        assert sorted(p["id"] for p in root["products"]) == ["old", "p1"]

    def test_forks_one_child_per_store(self, monkeypatch):
        _fake_s3_json_store(monkeypatch)
        products = {"p1": _product("p1", "s3://b/x.zarr", "v")}
        monkeypatch.setattr(
            generator, "discover_products", lambda api, base_url: products
        )

        async def fake_prewarm(urls):
            return {url: None for url in urls}

        monkeypatch.setattr(generator, "prewarm_stores", fake_prewarm)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_config=lambda: MagicMock(co_bucket="s3://bucket"),
                get_tiler_parquet_config=lambda: _tp_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store_url, uuid, variables, tp_config: calls.append(store_url)
            or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        assert calls == ["s3://b/x.zarr"]
