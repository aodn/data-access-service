"""Cron-triggered refresh: ``refresh_stores`` re-reads every currently-loaded
store's metadata.json sidecar, and does not let one store's failure stop
the sweep.
"""

from unittest.mock import MagicMock

import pytest

from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
)
from data_access_service.tiler.services.store.registry import (
    get_store,
    refresh_stores,
    store_registry,
)

STORE_URL = "s3://aodn-cloud-optimised/foo.zarr"
OUTPUT_DIR = "s3://my-bucket/tiler"


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


@pytest.fixture(autouse=True)
def output_dir(monkeypatch):
    import data_access_service.tiler.services.store.registry as registry_module

    monkeypatch.setattr(
        registry_module.Config.get_config(),
        "get_tiler_parquet_config",
        lambda: MagicMock(output_dir=OUTPUT_DIR),
    )
    s3_store: dict[str, dict] = {}

    def fake_read_json(path):
        if path not in s3_store:
            raise FileNotFoundError(f"{path!r} not found")
        return s3_store[path]

    monkeypatch.setattr(registry_module, "read_json", fake_read_json)
    return s3_store


def _meta(n_i: int) -> TilerParquetMetadata:
    return TilerParquetMetadata(
        uuid="u",
        dataset="foo.zarr",
        source_path=STORE_URL,
        n_i=n_i,
        n_j=1,
        lat=[float(x) for x in range(n_i)],
        lon=[0.0],
        timestamps=["2024-01-15T13:00:00.000000000Z"],
        variables={
            "v": TilerVariableMetadata(
                dtype="float32", attrs={}, parquet_path="v.parquet"
            )
        },
        schema_fingerprint="",
        generated_at="",
    )


def _write_metadata(s3_store: dict, dataset_stem: str, n_i: int) -> None:
    s3_store[f"{OUTPUT_DIR}/{dataset_stem}/metadata.json"] = _meta(n_i).to_dict()


def test_request_path_never_refreshes_an_already_loaded_store(output_dir):
    _write_metadata(output_dir, "foo", n_i=2)
    first = get_store(STORE_URL)
    for _ in range(10):
        assert get_store(STORE_URL) is first


def test_refresh_stores_rereads_every_loaded_store(output_dir):
    _write_metadata(output_dir, "foo", n_i=2)
    get_store(STORE_URL)

    _write_metadata(output_dir, "foo", n_i=5)  # sidecar changed on S3
    refresh_stores()

    assert get_store(STORE_URL).sizes["lat"] == 5


def test_refresh_publishes_a_new_dataset_object(output_dir):
    _write_metadata(output_dir, "foo", n_i=2)
    first = get_store(STORE_URL)

    _write_metadata(output_dir, "foo", n_i=2)
    refresh_stores()

    assert get_store(STORE_URL) is not first


def test_one_store_failure_does_not_stop_the_sweep(output_dir):
    _write_metadata(output_dir, "foo", n_i=2)
    _write_metadata(output_dir, "bar", n_i=3)
    get_store(STORE_URL)
    get_store("s3://aodn-cloud-optimised/bar.zarr")

    # "foo"'s sidecar becomes unreadable before the sweep runs.
    del output_dir[f"{OUTPUT_DIR}/foo/metadata.json"]

    refresh_stores()  # must not raise

    # "foo" keeps serving its last-known-good dataset...
    assert get_store(STORE_URL).sizes["lat"] == 2
    # ...while "bar"'s refresh still went through.
    assert store_registry.get("s3://aodn-cloud-optimised/bar.zarr").sizes["lat"] == 3


def test_refresh_stores_skips_stores_never_loaded():
    refresh_stores()  # nothing published yet; must not raise
    assert store_registry.time_index(STORE_URL) == {}


def test_clear_drops_loaded_stores(output_dir):
    _write_metadata(output_dir, "foo", n_i=2)
    get_store(STORE_URL)

    store_registry.clear()

    assert store_registry._metadata == {}
