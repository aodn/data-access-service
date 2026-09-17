"""Prewarm outcome reporting for the metadata.json-backed registry.

Prewarm reports what happened per URL and decides nothing; only the caller
can tell "one sidecar missing" from "the tiler is down".
"""

import json
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


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


@pytest.fixture(autouse=True)
def output_dir(tmp_path, monkeypatch):
    import data_access_service.tiler.services.store.registry as registry_module

    monkeypatch.setattr(
        registry_module.Config.get_config(),
        "get_tiler_parquet_config",
        lambda: MagicMock(output_dir=str(tmp_path)),
    )
    return tmp_path


def _meta(source_path: str, dataset: str) -> TilerParquetMetadata:
    return TilerParquetMetadata(
        version=1,
        uuid="u",
        dataset=dataset,
        source_path=source_path,
        n_i=1,
        n_j=1,
        lat=[0.0],
        lon=[0.0],
        timestamps=["2024-01-15T13:00:00.000000000Z"],
        variables={"v": TilerVariableMetadata(dtype="float32", attrs={})},
        schema_fingerprint="",
        generated_at="",
    )


def _write_metadata(output_dir, dataset_stem: str, source_path: str) -> None:
    d = output_dir / dataset_stem
    d.mkdir(parents=True, exist_ok=True)
    (d / "metadata.json").write_text(
        json.dumps(_meta(source_path, f"{dataset_stem}.zarr").to_dict())
    )


@pytest.mark.asyncio
async def test_successful_prewarm_reports_none_per_url(output_dir):
    _write_metadata(output_dir, "a", "s3://b/a.zarr")
    _write_metadata(output_dir, "b", "s3://b/b.zarr")

    outcomes = await prewarm_stores(["s3://b/a.zarr", "s3://b/b.zarr"])

    assert outcomes == {"s3://b/a.zarr": None, "s3://b/b.zarr": None}


@pytest.mark.asyncio
async def test_missing_sidecar_yields_file_not_found(output_dir):
    _write_metadata(output_dir, "ok", "s3://b/ok.zarr")

    outcomes = await prewarm_stores(["s3://b/ok.zarr", "s3://b/missing.zarr"])

    assert outcomes["s3://b/ok.zarr"] is None
    assert isinstance(outcomes["s3://b/missing.zarr"], FileNotFoundError)


@pytest.mark.asyncio
async def test_one_bad_url_does_not_block_the_others(output_dir):
    _write_metadata(output_dir, "ok1", "s3://b/ok1.zarr")
    _write_metadata(output_dir, "ok2", "s3://b/ok2.zarr")

    outcomes = await prewarm_stores(
        ["s3://b/bad.zarr", "s3://b/ok1.zarr", "s3://b/ok2.zarr"]
    )

    assert outcomes["s3://b/ok1.zarr"] is None
    assert outcomes["s3://b/ok2.zarr"] is None
    assert isinstance(outcomes["s3://b/bad.zarr"], FileNotFoundError)


def test_never_prewarmed_store_is_available_by_default():
    """Optimistic default: nothing has classified this URL as failed, so a
    caller that bypasses prewarm entirely (tests, a request racing startup)
    is not blocked by it."""
    assert is_store_available("s3://b/never-touched.zarr") is True


@pytest.mark.asyncio
async def test_successfully_loaded_store_is_available(output_dir):
    _write_metadata(output_dir, "ok", "s3://b/ok.zarr")

    await prewarm_stores(["s3://b/ok.zarr"])

    assert is_store_available("s3://b/ok.zarr") is True


@pytest.mark.asyncio
async def test_failed_store_is_unavailable(output_dir):
    await prewarm_stores(["s3://b/gone.zarr"])

    assert is_store_available("s3://b/gone.zarr") is False


@pytest.mark.asyncio
async def test_a_store_that_recovers_on_a_later_prewarm_becomes_available(output_dir):
    await prewarm_stores(["s3://b/flaky.zarr"])
    assert is_store_available("s3://b/flaky.zarr") is False

    # Sidecar appears (e.g. the batch job publishes it, then a later cron re-prewarm).
    _write_metadata(output_dir, "flaky", "s3://b/flaky.zarr")
    await prewarm_stores(["s3://b/flaky.zarr"])

    assert is_store_available("s3://b/flaky.zarr") is True


@pytest.mark.asyncio
async def test_prewarm_of_an_empty_url_list_is_a_no_op():
    assert await prewarm_stores([]) == {}
