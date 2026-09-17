import json
from unittest.mock import MagicMock

import pytest

from data_access_service.batch.tiler import generator
from data_access_service.batch.tiler.generator import (
    TilerParquetGenerationInProgressError,
    _group_by_store,
    generate_tiler_parquet_for_all_products,
    generate_tiler_parquet_for_store,
)
from data_access_service.models.tiler_types import TilerParquetConfig
from data_access_service.tiler.services.product.product import Product


def _product(pid: str, source_path: str, variable, uuid: str = "uuid-a") -> Product:
    return Product(
        id=pid, source_path=source_path, variable=variable, metadata_uuid=uuid
    )


def _tp_config(output_dir: str, **overrides) -> TilerParquetConfig:
    base = dict(
        output_dir=output_dir,
        batch_days=30,
        max_timestamps=5,
        use_fork_process=True,
    )
    base.update(overrides)
    return TilerParquetConfig(**base)


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


@pytest.fixture(autouse=True)
def stub_log_memory(monkeypatch):
    monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)


class TestGenerateForAllProducts:
    def test_skips_stores_that_fail_prewarm(self, monkeypatch, tmp_path):
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
                get_tiler_parquet_config=lambda: _tp_config(
                    str(tmp_path), use_fork_process=False
                ),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "build_tiler_parquet",
            lambda store_url, uuid, variables, tp_config: calls.append(store_url)
            or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        assert calls == ["s3://b/good.zarr"]
        # Only the successfully-converted store's product is published.
        root = json.loads((tmp_path / "root_metadata.json").read_text())
        assert [p["id"] for p in root["products"]] == ["p1"]

    def test_filters_by_uuid(self, monkeypatch, tmp_path):
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
                get_tiler_parquet_config=lambda: _tp_config(
                    str(tmp_path), use_fork_process=False
                ),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "build_tiler_parquet",
            lambda store_url, uuid, variables, tp_config: calls.append(store_url)
            or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-b")

        assert calls == ["s3://b/y.zarr"]

    def test_root_metadata_upserts_without_dropping_other_uuids(
        self, monkeypatch, tmp_path
    ):
        """A uuid-scoped run must not wipe out other uuids already published."""
        (tmp_path / "root_metadata.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "generated_at": "2020-01-01T00:00:00+00:00",
                    "products": [
                        _product(
                            "old", "s3://b/old.zarr", "v", uuid="uuid-old"
                        ).to_dict()
                    ],
                }
            )
        )
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
                get_tiler_parquet_config=lambda: _tp_config(
                    str(tmp_path), use_fork_process=False
                ),
            ),
        )
        monkeypatch.setattr(generator, "build_tiler_parquet", lambda *a, **k: True)

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-new")

        root = json.loads((tmp_path / "root_metadata.json").read_text())
        assert sorted(p["id"] for p in root["products"]) == ["old", "p1"]

    def test_forks_one_child_per_store_when_enabled(self, monkeypatch, tmp_path):
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
                get_tiler_parquet_config=lambda: _tp_config(
                    str(tmp_path), use_fork_process=True
                ),
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


class TestGenerateForStore:
    def test_rejects_concurrent_call(self, monkeypatch):
        generator._generation_lock.acquire()
        try:
            with pytest.raises(TilerParquetGenerationInProgressError):
                generate_tiler_parquet_for_store("s3://b/x.zarr", "uuid-a", ["v"])
        finally:
            generator._generation_lock.release()
