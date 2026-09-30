from unittest.mock import MagicMock

import pytest

from data_access_service.batch.tiler import generator
from data_access_service.batch.tiler.generator import (
    _group_by_store,
    generate_tiler_parquet_for_store,
    write_root_metadata,
)
from data_access_service.models.tiler_parquet_types import (
    ROOT_METADATA_VERSION,
    ProductIdentity,
    RootMetadata,
)
from data_access_service.models.tiler_types import (
    TilerBatchDuckDBConfig,
    TilerBatchConfig,
)


def _product(pid: str, store: str, variable, uuid: str = "uuid-a") -> ProductIdentity:
    return ProductIdentity(id=pid, store=store, variable=variable, metadata_uuid=uuid)


def _root_metadata(stores: dict[str, list[ProductIdentity]]) -> dict:
    """root_metadata.json content, as the fake S3 below holds it."""
    return RootMetadata(
        version=ROOT_METADATA_VERSION,
        generated_at="2020-01-01T00:00:00+00:00",
        stores=stores,
    ).to_dict()


def _published_ids(root: dict) -> list[str]:
    return sorted(p.id for p in RootMetadata.from_dict(root).products)


def _batch_config(
    tiler_root_dir: str = "s3://my-bucket/tiler", **overrides
) -> TilerBatchConfig:
    base = dict(
        tiler_root_dir=tiler_root_dir,
        max_chunks_per_run=2,
        regenerate_all=False,
        use_fork_process=True,
        duckdb=TilerBatchDuckDBConfig(
            memory_limit="256MB",
            threads=1,
            duckdb_temp_dir="test_tiler_parquet_tmp",
        ),
    )
    base.update(overrides)
    return TilerBatchConfig(**base)


def _stub_s3(monkeypatch, existing: dict | None, on_write) -> None:
    """Serve ``existing`` as root_metadata.json and pass each write to
    ``on_write(path, data)``."""
    monkeypatch.setattr(
        generator.storage,
        "read_json_with_etag",
        lambda aws, path: (existing, "etag" if existing is not None else None),
    )
    monkeypatch.setattr(
        generator.storage,
        "write_json_if_unchanged",
        lambda aws, path, data, etag: on_write(path, data),
    )


def _fake_s3_json_store(monkeypatch) -> dict[str, dict]:
    """In-memory stand-in for S3 objects, keyed by path — patches the
    storage read/write so write_root_metadata's upsert logic can be tested
    without real S3."""
    store: dict[str, dict] = {}
    monkeypatch.setattr(
        generator.storage,
        "read_json_with_etag",
        lambda aws, path: (store.get(path), "etag" if path in store else None),
    )
    monkeypatch.setattr(
        generator.storage,
        "write_json_if_unchanged",
        lambda aws, path, data, etag: store.__setitem__(path, data),
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
    """No real S3 here — the storage read/write are mocked, so this
    only checks write_root_metadata's own upsert logic against whatever
    the read returns."""

    def test_upserts_onto_existing_s3_content(self, monkeypatch):
        existing = _root_metadata({"old": [_product("old", "old", "v", uuid="old")]})
        written = {}
        _stub_s3(
            monkeypatch,
            existing,
            lambda path, data: written.update(path=path, data=data),
        )

        stores = {"new": [_product("new", "new", "v", uuid="uuid-new")]}
        path = write_root_metadata(stores, "s3://my-bucket/tiler")

        assert path == "s3://my-bucket/tiler/root_metadata.json"
        assert written["path"] == path
        assert _published_ids(written["data"]) == ["new", "old"]

    def test_replaces_a_stores_products_rather_than_merging_them(self, monkeypatch):
        """A variable spec dropped from config must disappear from the store."""
        existing = _root_metadata(
            {"x": [_product("p1", "x", "v"), _product("p2", "x", "w")]}
        )
        written = {}
        _stub_s3(monkeypatch, existing, lambda path, data: written.update(data=data))

        write_root_metadata({"x": [_product("p1", "x", "v")]}, "s3://my-bucket/tiler")

        assert _published_ids(written["data"]) == ["p1"]

    def test_skips_the_write_when_nothing_changed(self, monkeypatch):
        stores = {"x": [_product("p1", "x", "v")]}
        existing = _root_metadata(stores)
        writes = []
        _stub_s3(monkeypatch, existing, lambda path, data: writes.append(path))

        write_root_metadata(stores, "s3://my-bucket/tiler")

        assert writes == []

    def test_a_file_at_another_version_is_rewritten_from_scratch(self, monkeypatch):
        existing = _root_metadata({"old": [_product("old", "old", "v")]})
        existing["version"] = ROOT_METADATA_VERSION + 1
        written = {}
        _stub_s3(monkeypatch, existing, lambda path, data: written.update(data=data))

        write_root_metadata({"x": [_product("p1", "x", "v")]}, "s3://my-bucket/tiler")

        assert written["data"]["version"] == ROOT_METADATA_VERSION
        assert _published_ids(written["data"]) == ["p1"]

    def test_does_not_write_an_empty_catalogue(self, monkeypatch):
        writes = []
        _stub_s3(monkeypatch, None, lambda path, data: writes.append(path))

        write_root_metadata({}, "s3://my-bucket/tiler")

        assert writes == []

    def test_writes_fresh_manifest_when_nothing_exists_yet(self, monkeypatch):
        written = {}
        _stub_s3(monkeypatch, None, lambda path, data: written.update(data=data))

        write_root_metadata({"x": [_product("p1", "x", "v")]}, "s3://my-bucket/tiler")

        assert _published_ids(written["data"]) == ["p1"]
        assert written["data"]["stores"]["x"]["products"] == [
            {"id": "p1", "variable": "v", "metadata_uuid": "uuid-a"}
        ]

    def test_conflicting_write_rereads_and_keeps_the_other_jobs_store(
        self, monkeypatch
    ):
        """Another store job published between our read and write."""
        s3 = {"root": None}
        other = _root_metadata({"other": [_product("o", "other", "v")]})
        attempts = []

        def read(aws, path):
            return s3["root"], "etag" if s3["root"] else None

        def write(aws, path, data, etag):
            attempts.append(etag)
            if len(attempts) == 1:
                s3["root"] = other
                raise generator.storage.WriteConflict(path)
            s3["root"] = data

        monkeypatch.setattr(generator.storage, "read_json_with_etag", read)
        monkeypatch.setattr(generator.storage, "write_json_if_unchanged", write)
        monkeypatch.setattr(generator.time, "sleep", lambda s: None)

        write_root_metadata({"x": [_product("p1", "x", "v")]}, "s3://my-bucket/tiler")

        assert attempts == [None, "etag"]
        assert _published_ids(s3["root"]) == ["o", "p1"]


@pytest.fixture(autouse=True)
def stub_aws(monkeypatch):
    monkeypatch.setattr(generator, "AWSHelper", MagicMock)


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


class TestGenerateForStore:
    ROOT = "s3://my-bucket/tiler/root_metadata.json"

    def _setup(self, monkeypatch, build_ok=True, **config_overrides):
        s3_store = _fake_s3_json_store(monkeypatch)
        products = {
            "p1": _product("p1", "x", "v"),
            "p2": _product("p2", "y", "v", uuid="uuid-b"),
            "p3": _product("p3", "y", "w", uuid="uuid-b"),
        }
        monkeypatch.setattr(generator, "discover_products", lambda api: products)
        monkeypatch.setattr(
            generator,
            "config",
            MagicMock(get_tiler_batch_config=lambda: _batch_config(**config_overrides)),
        )
        calls = []
        monkeypatch.setattr(
            generator,
            "_build_in_subprocess",
            lambda store, uuid, variables, batch_config: calls.append(
                (store, uuid, variables)
            )
            or build_ok,
        )
        return s3_store, calls

    def test_converts_and_publishes_only_that_store(self, monkeypatch):
        s3_store, calls = self._setup(monkeypatch)

        assert generate_tiler_parquet_for_store(MagicMock(), "y") is True

        assert calls == [("y", "uuid-b", ["v", "w"])]
        assert _published_ids(s3_store[self.ROOT]) == ["p2", "p3"]

    def test_reports_failure_and_publishes_nothing(self, monkeypatch):
        s3_store, _ = self._setup(monkeypatch, build_ok=False)

        assert generate_tiler_parquet_for_store(MagicMock(), "y") is False
        assert s3_store == {}

    def test_a_failed_store_keeps_its_previous_entry(self, monkeypatch):
        """Its parquet from an earlier run is still on S3 and still serveable."""
        s3_store, _ = self._setup(monkeypatch, build_ok=False)
        s3_store[self.ROOT] = _root_metadata({"y": [_product("p2", "y", "v")]})

        generate_tiler_parquet_for_store(MagicMock(), "y")

        assert _published_ids(s3_store[self.ROOT]) == ["p2"]

    def test_publishing_keeps_other_stores(self, monkeypatch):
        s3_store, _ = self._setup(monkeypatch)
        s3_store[self.ROOT] = _root_metadata(
            {"old": [_product("old", "old", "v", uuid="uuid-old")]}
        )

        generate_tiler_parquet_for_store(MagicMock(), "y")

        assert _published_ids(s3_store[self.ROOT]) == ["old", "p2", "p3"]

    def test_unknown_store_does_nothing(self, monkeypatch):
        _, calls = self._setup(monkeypatch)

        assert generate_tiler_parquet_for_store(MagicMock(), "z") is True
        assert calls == []

    def test_store_with_no_timestamps_is_not_published(self, monkeypatch):
        s3_store, _ = self._setup(monkeypatch)
        monkeypatch.setattr(
            generator, "read_metadata", lambda root, store: MagicMock(timestamps=[])
        )

        generate_tiler_parquet_for_store(MagicMock(), "y")

        assert s3_store == {}

    def test_store_that_lost_its_data_is_removed(self, monkeypatch):
        s3_store, _ = self._setup(monkeypatch)
        s3_store[self.ROOT] = _root_metadata(
            {"x": [_product("p1", "x", "v")], "y": [_product("p2", "y", "v")]}
        )
        monkeypatch.setattr(
            generator, "read_metadata", lambda root, store: MagicMock(timestamps=[])
        )

        generate_tiler_parquet_for_store(MagicMock(), "y")

        assert _published_ids(s3_store[self.ROOT]) == ["p1"]

    def test_runs_in_process_without_forking(self, monkeypatch):
        s3_store, _ = self._setup(monkeypatch, use_fork_process=False)

        def no_fork(*a, **k):
            raise AssertionError("must not fork")

        monkeypatch.setattr(generator, "_build_in_subprocess", no_fork)
        calls = []
        monkeypatch.setattr(
            generator,
            "build_tiler_parquet",
            lambda store, uuid, variables, batch_config: calls.append(store) or True,
        )

        assert generate_tiler_parquet_for_store(MagicMock(), "y") is True
        assert calls == ["y"]
        assert _published_ids(s3_store[self.ROOT]) == ["p2", "p3"]


class TestSubmitStoreJobs:
    def test_submits_one_job_per_store(self, monkeypatch):
        products = {
            "p1": _product("p1", "x", "v"),
            "p2": _product("p2", "x", "w"),
            "p3": _product("p3", "y.v2", "v", uuid="uuid-b"),
        }
        monkeypatch.setattr(generator, "discover_products", lambda api: products)
        aws = MagicMock()
        aws.submit_a_job.side_effect = lambda **kw: f"job-{kw['parameters']['store']}"
        monkeypatch.setattr(generator, "AWSHelper", lambda: aws)
        converted = []
        monkeypatch.setattr(generator, "_build_in_subprocess", converted.append)

        job_ids = generator.submit_store_jobs(
            api=MagicMock(), job_queue="q", job_definition="d"
        )

        assert job_ids == ["job-x", "job-y.v2"]
        submitted = [c.kwargs for c in aws.submit_a_job.call_args_list]
        assert submitted == [
            dict(
                job_name="tiler-parquet-x",
                job_queue="q",
                job_definition="d",
                parameters={
                    "type": "generate-tiler-parquet-store",
                    "store": "x",
                },
            ),
            dict(
                job_name="tiler-parquet-y-v2",
                job_queue="q",
                job_definition="d",
                parameters={
                    "type": "generate-tiler-parquet-store",
                    "store": "y.v2",
                },
            ),
        ]
        # The dispatcher converts nothing itself.
        assert converted == []


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
        assert seen["regenerate_all"] is False

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
