"""Tiler warmup sequencing and readiness.

The shape being defended: every product in ``root_metadata.json`` is
published after its store's metadata is loaded, even when that load fails,
and a store failing to load never keeps the tiler unready.
"""

import asyncio
import threading

import pytest

from data_access_service.core.tiler_routes import shared, startup
from data_access_service.core.tiler_routes.startup import run_tiler_warmup
from data_access_service.tiler.services.product.product import Product

# --- warmup sequencing ------------------------------------------------------


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


@pytest.mark.asyncio
async def test_missing_root_metadata_leaves_the_tiler_unready(
    warmup_env, monkeypatch, caplog
):
    calls, state = warmup_env

    def boom():
        raise FileNotFoundError("root_metadata.json not found")

    monkeypatch.setattr(startup, "_load_catalog", boom)

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup()

    assert state["ready"] is False
    assert "publish" not in calls
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


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
@pytest.mark.parametrize(
    "failing_step",
    [
        "_load_catalog",
        "load_products",
    ],
)
async def test_any_fatal_step_leaves_readiness_false(
    warmup_env, failing_step, monkeypatch, caplog
):
    calls, state = warmup_env

    def boom(*args, **kwargs):
        raise RuntimeError(f"{failing_step} exploded")

    monkeypatch.setattr(startup, failing_step, boom)

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup()

    assert state["ready"] is False
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


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

    assert not any("Tiler warmup failed" in r.message for r in caplog.records)


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
