from unittest.mock import MagicMock

import pytest

from data_access_service.batch.tiler import generator
from data_access_service.batch.tiler.generator import (
    _group_by_store,
    generate_tiler_parquet_for_all_products,
    write_root_metadata,
)
from data_access_service.models.tiler_parquet_types import ProductIdentity
from data_access_service.models.tiler_types import (
    TilerBatchDuckDBConfig,
    TilerBatchConfig,
)


def _product(pid: str, store: str, variable, uuid: str = "uuid-a") -> ProductIdentity:
    return ProductIdentity(id=pid, store=store, variable=variable, metadata_uuid=uuid)


def _batch_config(
    tiler_root_dir: str = "s3://my-bucket/tiler", **overrides
) -> TilerBatchConfig:
    base = dict(
        tiler_root_dir=tiler_root_dir,
        max_chunks_per_run=2,
        use_fork_process=True,
        duckdb=TilerBatchDuckDBConfig(
            memory_limit="256MB",
            threads=1,
            temp_dir_prefix="test_tiler_parquet_tmp",
        ),
    )
    base.update(overrides)
    return TilerBatchConfig(**base)


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
            "p1": _product("p1", "x", "GSL"),
            "p2": _product("p2", "x", "GSLA"),
            "p3": _product("p3", "y", ["UCUR", "VCUR"], uuid="uuid-b"),
        }
        grouped = _group_by_store(products)
        assert grouped == {
            "x": ("uuid-a", ["GSL", "GSLA"]),
            "y": ("uuid-b", ["UCUR", "VCUR"]),
        }


class TestWriteRootMetadataToS3:
    """No real S3 here — storage.read_json/write_json are mocked, so this
    only checks write_root_metadata's own upsert logic against whatever
    storage.read_json returns."""

    def test_upserts_onto_existing_s3_content(self, monkeypatch):
        existing = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [_product("old", "old", "v", uuid="uuid-old").to_dict()],
        }
        monkeypatch.setattr(generator.storage, "read_json", lambda path: existing)
        written = {}
        monkeypatch.setattr(
            generator.storage,
            "write_json",
            lambda path, data: written.update(path=path, data=data),
        )

        products = [_product("new", "new", "v", uuid="uuid-new")]
        path = write_root_metadata(products, "s3://my-bucket/tiler")

        assert path == "s3://my-bucket/tiler/root_metadata.json"
        assert written["path"] == path
        assert sorted(p["id"] for p in written["data"]["products"]) == ["new", "old"]

    def test_skips_the_write_when_nothing_changed(self, monkeypatch):
        products = [_product("p1", "x", "v")]
        existing = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [p.to_dict() for p in products],
        }
        monkeypatch.setattr(generator.storage, "read_json", lambda path: existing)
        writes = []
        monkeypatch.setattr(
            generator.storage, "write_json", lambda path, data: writes.append(path)
        )

        write_root_metadata(products, "s3://my-bucket/tiler")

        assert writes == []

    def test_does_not_write_an_empty_catalogue(self, monkeypatch):
        monkeypatch.setattr(generator.storage, "read_json", lambda path: None)
        writes = []
        monkeypatch.setattr(
            generator.storage, "write_json", lambda path, data: writes.append(path)
        )

        write_root_metadata([], "s3://my-bucket/tiler")

        assert writes == []

    def test_writes_fresh_manifest_when_nothing_exists_yet(self, monkeypatch):
        monkeypatch.setattr(generator.storage, "read_json", lambda path: None)
        written = {}
        monkeypatch.setattr(
            generator.storage,
            "write_json",
            lambda path, data: written.update(data=data),
        )

        products = [_product("p1", "x", "v")]
        write_root_metadata(products, "s3://my-bucket/tiler")

        assert [p["id"] for p in written["data"]["products"]] == ["p1"]


@pytest.fixture(autouse=True)
def stub_log_memory(monkeypatch):
    monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def every_store_has_data(monkeypatch):
    """Default: each synced store's sidecar lists a timestamp, so it is
    published. TestPublishing overrides this."""
    monkeypatch.setattr(
        generator,
        "read_metadata",
        lambda tiler_root_dir, store: MagicMock(timestamps=["t"]),
    )


class TestGenerateForAllProducts:
    def test_publishes_only_stores_that_succeeded(self, monkeypatch):
        s3_store = _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "good", "v"),
            "p2": _product("p2", "bad", "v", uuid="uuid-b"),
        }
        monkeypatch.setattr(generator, "discover_products", lambda api: products)

        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_batch_config=lambda: _batch_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store, uuid, variables, batch_config: calls.append(store)
            or store == "good",
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        # Every store is attempted, one after another, in a stable order.
        assert calls == ["bad", "good"]
        # Only the successfully-converted store's product is published.
        root = s3_store["s3://my-bucket/tiler/root_metadata.json"]
        assert [p["id"] for p in root["products"]] == ["p1"]

    def test_filters_by_uuid(self, monkeypatch):
        _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "x", "v", uuid="uuid-a"),
            "p2": _product("p2", "y", "v", uuid="uuid-b"),
        }
        monkeypatch.setattr(generator, "discover_products", lambda api: products)

        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_batch_config=lambda: _batch_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store, uuid, variables, batch_config: calls.append(store) or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-b")

        assert calls == ["y"]

    def test_root_metadata_upserts_without_dropping_other_uuids(self, monkeypatch):
        """A uuid-scoped run must not wipe out other uuids already published."""
        s3_store = _fake_s3_json_store(monkeypatch)
        s3_store["s3://my-bucket/tiler/root_metadata.json"] = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [_product("old", "old", "v", uuid="uuid-old").to_dict()],
        }
        products = {"p1": _product("p1", "new", "v", uuid="uuid-new")}
        monkeypatch.setattr(generator, "discover_products", lambda api: products)

        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_batch_config=lambda: _batch_config(),
            ),
        )
        monkeypatch.setattr(generator, "_build_in_subprocess", lambda *a, **k: True)

        generate_tiler_parquet_for_all_products(api=MagicMock(), uuid="uuid-new")

        root = s3_store["s3://my-bucket/tiler/root_metadata.json"]
        assert sorted(p["id"] for p in root["products"]) == ["old", "p1"]

    def test_forks_one_child_per_store(self, monkeypatch):
        _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "x", "v"),
            "p2": _product("p2", "x", "w"),
            "p3": _product("p3", "y", "v"),
        }
        monkeypatch.setattr(generator, "discover_products", lambda api: products)

        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_batch_config=lambda: _batch_config(),
            ),
        )

        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store, uuid, variables, batch_config: calls.append(store) or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        assert calls == ["x", "y"]


class TestPublishing:
    def _run(self, monkeypatch, s3_store, products, sidecars):
        """sidecars: {store: [timestamps]} as each store's sidecar reads back."""
        monkeypatch.setattr(
            generator,
            "read_metadata",
            lambda tiler_root_dir, store: (
                MagicMock(timestamps=sidecars[store]) if store in sidecars else None
            ),
        )
        monkeypatch.setattr(generator, "discover_products", lambda api: products)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(get_tiler_batch_config=lambda: _batch_config()),
        )
        monkeypatch.setattr(generator, "_build_in_subprocess", lambda *a, **k: True)
        generate_tiler_parquet_for_all_products(api=MagicMock())
        return s3_store["s3://my-bucket/tiler/root_metadata.json"]

    def test_store_with_no_timestamps_is_not_published(self, monkeypatch):
        s3_store = _fake_s3_json_store(monkeypatch)
        products = {"p1": _product("p1", "x", "v"), "p2": _product("p2", "y", "v")}

        root = self._run(monkeypatch, s3_store, products, {"x": ["t1"], "y": []})

        assert [p["id"] for p in root["products"]] == ["p1"]

    def test_store_that_lost_its_data_is_removed(self, monkeypatch):
        s3_store = _fake_s3_json_store(monkeypatch)
        s3_store["s3://my-bucket/tiler/root_metadata.json"] = {
            "version": 1,
            "generated_at": "2020-01-01T00:00:00+00:00",
            "products": [
                _product("p1", "x", "v").to_dict(),
                _product("p2", "y", "v").to_dict(),
            ],
        }
        products = {"p1": _product("p1", "x", "v"), "p2": _product("p2", "y", "v")}

        root = self._run(monkeypatch, s3_store, products, {"x": ["t1"], "y": []})

        assert [p["id"] for p in root["products"]] == ["p1"]


class TestInProcessMode:
    def test_runs_each_store_in_process_without_forking(self, monkeypatch):
        s3_store = _fake_s3_json_store(monkeypatch)
        products = {"p1": _product("p1", "x", "v"), "p2": _product("p2", "y", "v")}
        monkeypatch.setattr(generator, "discover_products", lambda api: products)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(
                get_tiler_batch_config=lambda: _batch_config(use_fork_process=False)
            ),
        )

        def no_fork(*a, **k):
            raise AssertionError("must not fork")

        monkeypatch.setattr(generator, "_build_in_subprocess", no_fork)
        calls = []
        monkeypatch.setattr(
            generator,
            "build_tiler_parquet",
            lambda store, uuid, variables, batch_config: calls.append(store) or True,
        )

        generate_tiler_parquet_for_all_products(api=MagicMock())

        assert calls == ["x", "y"]
        root = s3_store["s3://my-bucket/tiler/root_metadata.json"]
        assert sorted(p["id"] for p in root["products"]) == ["p1", "p2"]


class TestBuildTilerParquet:
    def test_store_that_fails_to_open_is_not_synced(self, monkeypatch):
        monkeypatch.setattr(generator, "open_store", lambda store: ValueError("x"))
        synced = []
        monkeypatch.setattr(generator, "sync_store", lambda *a, **k: synced.append(a))

        assert generator.build_tiler_parquet("x", "u", ["v"], _batch_config()) is False
        assert synced == []

    def test_passes_the_chunk_limit_through(self, monkeypatch):
        monkeypatch.setattr(generator, "open_store", lambda store: None)
        seen = {}

        def fake_sync(store, uuid, variables, tiler_root_dir, **kwargs):
            seen.update(kwargs)
            return [], "path"

        monkeypatch.setattr(generator, "sync_store", fake_sync)

        assert generator.build_tiler_parquet("x", "u", ["v"], _batch_config()) is True
        assert seen["max_chunks_per_run"] == 2

    def test_store_handle_is_released_afterwards(self, monkeypatch):
        monkeypatch.setattr(generator, "open_store", lambda store: None)
        monkeypatch.setattr(generator, "sync_store", lambda *a, **k: ([], "path"))
        closed = []
        monkeypatch.setattr(generator, "close_store", closed.append)

        generator.build_tiler_parquet("x", "u", ["v"], _batch_config())

        assert closed == ["x"]

    def test_sync_error_is_reported_not_raised(self, monkeypatch):
        monkeypatch.setattr(generator, "open_store", lambda store: None)

        def boom(*a, **k):
            raise RuntimeError("s3 down")

        monkeypatch.setattr(generator, "sync_store", boom)

        assert generator.build_tiler_parquet("x", "u", ["v"], _batch_config()) is False
