"""load_stores outcome reporting for the metadata.json-backed registry.

load_stores reports what happened per store and decides nothing; only the caller
can tell "one sidecar missing" from "the tiler is down".
"""

import pytest

from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
)
from data_access_service.tiler.services.store.registry import (
    is_store_available,
    load_stores,
    retain_stores,
    store_registry,
)

OUTPUT_DIR = "s3://my-bucket/tiler"


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


@pytest.fixture(autouse=True)
def tiler_root_dir(monkeypatch):
    import data_access_service.tiler.services.store.registry as registry_module

    monkeypatch.setattr(
        registry_module.Config.get_config(),
        "get_tiler_root_dir",
        lambda: OUTPUT_DIR,
    )
    s3_store: dict[str, dict] = {}

    def fake_read_json(path):
        if path not in s3_store:
            raise FileNotFoundError(f"{path!r} not found")
        return s3_store[path]

    monkeypatch.setattr(registry_module, "read_json", fake_read_json)
    return s3_store


def _meta(store: str) -> TilerParquetMetadata:
    return TilerParquetMetadata(
        uuid="u",
        dataset=f"{store}.zarr",
        n_i=1,
        n_j=1,
        lat=[0.0],
        lon=[0.0],
        timestamps=["2024-01-15T13:00:00.000000000Z"],
        variables={"v": TilerVariableMetadata(dtype="float32", attrs={})},
        generated_at="",
    )


def _write_metadata(s3_store: dict, store: str) -> None:
    s3_store[f"{OUTPUT_DIR}/{store}/metadata.json"] = _meta(store).to_dict()


def test_successful_load_reports_none_per_store(tiler_root_dir):
    _write_metadata(tiler_root_dir, "a")
    _write_metadata(tiler_root_dir, "b")

    outcomes = load_stores(["a", "b"])

    assert outcomes == {"a": None, "b": None}


def test_missing_sidecar_yields_file_not_found(tiler_root_dir):
    _write_metadata(tiler_root_dir, "ok")

    outcomes = load_stores(["ok", "missing"])

    assert outcomes["ok"] is None
    assert isinstance(outcomes["missing"], FileNotFoundError)


def test_one_bad_store_does_not_block_the_others(tiler_root_dir):
    _write_metadata(tiler_root_dir, "ok1")
    _write_metadata(tiler_root_dir, "ok2")

    outcomes = load_stores(["bad", "ok1", "ok2"])

    assert outcomes["ok1"] is None
    assert outcomes["ok2"] is None
    assert isinstance(outcomes["bad"], FileNotFoundError)


def test_never_loaded_store_is_unavailable():
    """Every catalogue store is loaded before its products are published, so
    one that is not loaded is treated as failed."""
    assert is_store_available("never-touched") is False


def test_successfully_loaded_store_is_available(tiler_root_dir):
    _write_metadata(tiler_root_dir, "ok")

    load_stores(["ok"])

    assert is_store_available("ok") is True


def test_failed_store_is_unavailable(tiler_root_dir):
    load_stores(["gone"])

    assert is_store_available("gone") is False


def test_a_store_that_recovers_on_a_later_load_becomes_available(tiler_root_dir):
    load_stores(["flaky"])
    assert is_store_available("flaky") is False

    # Sidecar appears (e.g. the batch job publishes it, then a later scheduled refresh).
    _write_metadata(tiler_root_dir, "flaky")
    load_stores(["flaky"])

    assert is_store_available("flaky") is True


def test_load_of_an_empty_store_list_is_a_no_op():
    assert load_stores([]) == {}


def test_retain_forgets_stores_not_kept(tiler_root_dir):
    _write_metadata(tiler_root_dir, "keep")
    _write_metadata(tiler_root_dir, "gone")
    load_stores(["keep", "gone", "broken"])

    retain_stores({"keep"})

    assert store_registry.time_index("keep")
    assert store_registry.time_index("gone") == {}
    assert is_store_available("gone") is False
