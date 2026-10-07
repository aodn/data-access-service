"""Tiler warmup sequencing and readiness.

The shape being defended: products whose ``metadata.json`` is missing are
left unpublished, any other store-load failure still publishes the product,
and a store failing to load never keeps the tiler unready.
"""

import asyncio
import threading

import pytest
from tenacity import wait_none

from data_access_service.core.tiler_routes import shared, startup
from data_access_service.core.tiler_routes.startup import run_tiler_warmup
from data_access_service.tiler.services.product.product import Product

# --- warmup sequencing ------------------------------------------------------


@pytest.fixture(autouse=True)
def no_catalog_retry_wait(monkeypatch):
    """Keep the catalogue retries, drop their backoff, so failure tests stay fast."""
    monkeypatch.setattr(startup._refresh_catalog_with_retry.retry, "wait", wait_none())


@pytest.fixture
def warmup_env(monkeypatch):
    """Stub every step around root-metadata loading so ordering can be observed directly."""
    calls: list[str] = []
    state = {
        "candidates": {"a:v": Product(id="a:v", store="a", variable="v")},
        "outcomes": {"a": None},
        "published": None,
        "ready": False,
    }

    def record(name, value=None):
        def _fn(*args, **kwargs):
            calls.append(name)
            return value

        return _fn

    def fake_load_stores(stores):
        calls.append("load_stores")
        state["loaded_stores"] = stores
        return state["outcomes"]

    def fake_publish(products):
        calls.append("publish")
        state["published"] = products

    def fake_mark_ready():
        calls.append("mark_ready")
        state["ready"] = True

    monkeypatch.setattr(
        startup,
        "_load_catalog",
        lambda: (calls.append("load_catalog"), state["candidates"])[1],
    )
    monkeypatch.setattr(startup, "load_colormaps", record("colormaps"))
    monkeypatch.setattr(startup, "warmup_kernels", record("kernels"))
    monkeypatch.setattr(startup, "warmup_visual", record("visual"))
    monkeypatch.setattr(startup, "load_stores", fake_load_stores)
    monkeypatch.setattr(startup, "load_products", fake_publish)
    monkeypatch.setattr(startup, "mark_tiler_ready", fake_mark_ready)

    return calls, state


@pytest.mark.asyncio
async def test_happy_path_loads_stores_then_publishes_then_marks_ready(warmup_env):
    calls, state = warmup_env
    await run_tiler_warmup()

    assert state["ready"] is True
    assert state["published"] == state["candidates"]
    # Stores load first, so a published product's store is never unknown.
    assert calls.index("load_catalog") < calls.index("load_stores")
    assert calls.index("load_stores") < calls.index("publish")
    assert calls.index("publish") < calls.index("mark_ready")
    # Render warmup follows readiness, so a kernel failure cannot hold routes at 503.
    assert calls.index("mark_ready") < calls.index("colormaps")


@pytest.mark.asyncio
async def test_catalogue_load_retries_until_it_succeeds(
    warmup_env, monkeypatch, caplog
):
    calls, state = warmup_env
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise FileNotFoundError("root_metadata.json not found")
        calls.append("load_catalog")
        return state["candidates"]

    monkeypatch.setattr(startup, "_load_catalog", flaky)

    with caplog.at_level("WARNING"):
        await run_tiler_warmup()

    assert attempts["n"] == 3
    assert state["ready"] is True
    assert state["published"] == state["candidates"]
    assert calls.count("colormaps") == 1
    assert any(
        "[Retry] Tiler catalogue load failed" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_load_stores_receives_every_unique_candidate_store(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "a:w": Product(id="a:w", store="a", variable="w"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }
    state["outcomes"] = {"a": None, "b": None}

    await run_tiler_warmup()

    # Deduplicated and sorted — 3 products but only 2 opens.
    assert state["loaded_stores"] == ["a", "b"]


@pytest.mark.asyncio
async def test_all_candidates_are_published_even_with_a_failed_store(warmup_env):
    """A store failing to load no longer withholds its products from the
    registry — that is now enforced per-request, not by publication."""
    calls, state = warmup_env
    state["outcomes"] = {"a": RuntimeError("s3 down")}

    await run_tiler_warmup()

    assert state["published"] == state["candidates"]
    assert "publish" in calls


@pytest.mark.asyncio
async def test_every_store_failing_still_reaches_ready(warmup_env):
    """Failed stores are retried by the scheduled refresh, so they must not
    leave the tiler unready until a restart."""
    calls, state = warmup_env
    state["outcomes"] = {"a": RuntimeError("s3 down")}

    await run_tiler_warmup()

    assert state["ready"] is True
    assert "publish" in calls


@pytest.mark.asyncio
async def test_missing_metadata_json_skips_that_product(warmup_env, caplog):
    """A batch store job has not written metadata.json yet. Publish the
    stores that have one, and leave the others out of the catalogue."""
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }
    state["outcomes"] = {"a": None, "b": FileNotFoundError("metadata.json not found")}

    with caplog.at_level("WARNING"):
        await run_tiler_warmup()

    assert state["ready"] is True
    assert state["published"] == {"a:v": state["candidates"]["a:v"]}
    assert any("Skipping" in r.message and "b:v" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_every_metadata_json_missing_stays_ready_without_publishing(warmup_env):
    calls, state = warmup_env
    state["outcomes"] = {"a": FileNotFoundError("metadata.json not found")}

    await run_tiler_warmup()

    assert state["ready"] is True
    assert state["published"] is None
    assert "publish" not in calls


@pytest.mark.asyncio
async def test_a_partial_store_failure_still_reaches_ready(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }
    state["outcomes"] = {"a": None, "b": RuntimeError("down")}

    await run_tiler_warmup()

    assert state["ready"] is True
    assert "mark_ready" in calls


@pytest.mark.asyncio
async def test_render_warmup_failure_stays_ready(warmup_env, monkeypatch, caplog):
    """numba or GDAL failing must not un-publish a catalogue that already loaded."""
    calls, state = warmup_env

    def boom():
        calls.append("kernels")
        raise RuntimeError("numba exploded")

    monkeypatch.setattr(startup, "warmup_kernels", boom)

    with caplog.at_level("ERROR"):
        await run_tiler_warmup()

    assert state["ready"] is True
    assert calls.count("load_catalog") == 1
    assert any("Tiler render warmup failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_is_re_raised_not_logged_as_failure(
    warmup_env, monkeypatch, caplog
):
    """Warmup runs as a lifespan task whose result is never awaited. Swallowing
    CancelledError would turn every shutdown into a spurious CRITICAL."""
    calls, state = warmup_env

    def cancelled():
        raise asyncio.CancelledError()

    monkeypatch.setattr(startup, "load_colormaps", cancelled)

    with caplog.at_level("CRITICAL"):
        with pytest.raises(asyncio.CancelledError):
            await run_tiler_warmup()

    assert not any("Tiler render warmup failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_catalogue_cancellation_is_not_retried(warmup_env, monkeypatch, caplog):
    calls, state = warmup_env

    def cancelled():
        raise asyncio.CancelledError()

    monkeypatch.setattr(startup, "_load_catalog", cancelled)

    with caplog.at_level("WARNING"):
        with pytest.raises(asyncio.CancelledError):
            await run_tiler_warmup()

    assert state["ready"] is False
    assert not any("[Retry]" in r.message for r in caplog.records)


@pytest.fixture(autouse=True)
def restore_tiler_readiness():
    """run_tiler_warmup flips module-level readiness; put it back afterwards."""
    saved = shared._tiler_ready
    yield
    shared._tiler_ready = saved


def test_refresh_catalog_publishes_and_loads_the_new_stores(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }

    products, _ = startup.refresh_catalog()

    assert products == state["candidates"]
    assert state["published"] == state["candidates"]
    assert state["loaded_stores"] == ["a", "b"]


@pytest.mark.asyncio
async def test_catalogue_is_loaded_off_the_event_loop(warmup_env, monkeypatch):
    loop_thread = threading.current_thread()
    seen = []
    monkeypatch.setattr(
        startup,
        "load_stores",
        lambda stores: seen.append(threading.current_thread()) or {"a": None},
    )

    await run_tiler_warmup()

    assert seen and seen[0] is not loop_thread


def test_refresh_catalog_forgets_removed_stores(warmup_env, monkeypatch):
    calls, state = warmup_env
    kept = []
    monkeypatch.setattr(startup, "retain_stores", kept.append)
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }

    startup.refresh_catalog()

    assert kept == [{"a", "b"}]


# --- refresh_tiler ----------------------------------------------------------


def test_refresh_tiler_refreshes_stores_then_catalogue(monkeypatch):
    calls = []
    monkeypatch.setattr(startup, "refresh_stores", lambda: calls.append("stores"))
    monkeypatch.setattr(
        startup, "refresh_catalog", lambda: calls.append("catalog") or ({}, {})
    )

    assert startup.refresh_tiler() == ({}, {})
    assert calls == ["stores", "catalog"]


def test_refresh_tiler_rejects_concurrent_refresh(monkeypatch):
    monkeypatch.setattr(startup, "refresh_stores", lambda: None)
    monkeypatch.setattr(startup, "refresh_catalog", lambda: ({}, {}))

    with startup._refresh_lock:
        with pytest.raises(startup.RefreshInProgressError):
            startup.refresh_tiler()

    startup.refresh_tiler()  # lock released, runs again


def test_refresh_tiler_releases_lock_on_error(monkeypatch):
    monkeypatch.setattr(startup, "refresh_stores", lambda: None)

    def boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(startup, "refresh_catalog", boom)

    with pytest.raises(RuntimeError):
        startup.refresh_tiler()
    assert not startup._refresh_lock.locked()
