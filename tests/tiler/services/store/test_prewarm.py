"""Prewarm outcome reporting for the metadata.json-backed registry.

Prewarm reports what happened per store and decides nothing; only the caller
can tell "one sidecar missing" from "the tiler is down".
"""

from unittest.mock import MagicMock

import pytest

from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
)
from data_access_service.tiler.services.store.registry import (
    is_store_available,
    prewarm_stores,
    store_registry,
)

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
        schema_fingerprint="",
        generated_at="",
    )


def _write_metadata(s3_store: dict, store: str) -> None:
    s3_store[f"{OUTPUT_DIR}/{store}/metadata.json"] = _meta(store).to_dict()


@pytest.mark.asyncio
async def test_successful_prewarm_reports_none_per_store(output_dir):
    _write_metadata(output_dir, "a")
    _write_metadata(output_dir, "b")

    outcomes = await prewarm_stores(["a", "b"])

    assert outcomes == {"a": None, "b": None}


@pytest.mark.asyncio
async def test_missing_sidecar_yields_file_not_found(output_dir):
    _write_metadata(output_dir, "ok")

    outcomes = await prewarm_stores(["ok", "missing"])

    assert outcomes["ok"] is None
    assert isinstance(outcomes["missing"], FileNotFoundError)


@pytest.mark.asyncio
async def test_one_bad_store_does_not_block_the_others(output_dir):
    _write_metadata(output_dir, "ok1")
    _write_metadata(output_dir, "ok2")

    outcomes = await prewarm_stores(["bad", "ok1", "ok2"])

    assert outcomes["ok1"] is None
    assert outcomes["ok2"] is None
    assert isinstance(outcomes["bad"], FileNotFoundError)


def test_never_prewarmed_store_is_available_by_default():
    """Optimistic default: nothing has classified this store as failed, so a
    caller that bypasses prewarm entirely (tests, a request racing startup)
    is not blocked by it."""
    assert is_store_available("never-touched") is True


@pytest.mark.asyncio
async def test_successfully_loaded_store_is_available(output_dir):
    _write_metadata(output_dir, "ok")

    await prewarm_stores(["ok"])

    assert is_store_available("ok") is True


@pytest.mark.asyncio
async def test_failed_store_is_unavailable(output_dir):
    await prewarm_stores(["gone"])

    assert is_store_available("gone") is False


@pytest.mark.asyncio
async def test_a_store_that_recovers_on_a_later_prewarm_becomes_available(output_dir):
    await prewarm_stores(["flaky"])
    assert is_store_available("flaky") is False

    # Sidecar appears (e.g. the batch job publishes it, then a later cron re-prewarm).
    _write_metadata(output_dir, "flaky")
    await prewarm_stores(["flaky"])

    assert is_store_available("flaky") is True


@pytest.mark.asyncio
async def test_prewarm_of_an_empty_store_list_is_a_no_op():
    assert await prewarm_stores([]) == {}
