"""Tiler warmup sequencing and readiness.

The shape being defended: every product in ``root_metadata.json`` is
published up front — nothing waits on its store's sidecar loading — but the
tiler still exits unready, without ``mark_tiler_ready()``, if every store
fails to load.
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

    def fake_prewarm(urls):
        calls.append("prewarm")
        state["prewarm_urls"] = urls
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
    monkeypatch.setattr(startup, "warmup_resample", record("resample"))
    monkeypatch.setattr(startup, "warmup_visual", record("visual"))
    monkeypatch.setattr(startup, "prewarm_stores", fake_prewarm)
    monkeypatch.setattr(startup, "load_products", fake_publish)
    monkeypatch.setattr(startup, "mark_tiler_ready", fake_mark_ready)

    return calls, state


@pytest.mark.asyncio
async def test_happy_path_publishes_then_prewarms_then_marks_ready(warmup_env):
    calls, state = warmup_env
    await run_tiler_warmup()

    assert state["ready"] is True
    assert state["published"] == state["candidates"]
    # Publication does not wait on store health.
    assert calls.index("load_catalog") < calls.index("publish")
    assert calls.index("publish") < calls.index("prewarm")
    assert calls.index("prewarm") < calls.index("mark_ready")


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
async def test_prewarm_receives_every_unique_candidate_store(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", store="a", variable="v"),
        "a:w": Product(id="a:w", store="a", variable="w"),
        "b:v": Product(id="b:v", store="b", variable="v"),
    }
    state["outcomes"] = {"a": None, "b": None}

    await run_tiler_warmup()

    # Deduplicated and sorted — 3 products but only 2 opens.
    assert state["prewarm_urls"] == ["a", "b"]


@pytest.mark.asyncio
async def test_all_candidates_are_published_even_with_a_failed_store(warmup_env):
    """A store failing prewarm no longer withholds its products from the
    registry — that is now enforced per-request, not by publication."""
    calls, state = warmup_env
    state["outcomes"] = {"a": RuntimeError("s3 down")}

    await run_tiler_warmup()

    assert state["published"] == state["candidates"]
    assert "publish" in calls


@pytest.mark.asyncio
async def test_every_store_failing_leaves_the_tiler_unready(warmup_env, caplog):
    calls, state = warmup_env
    state["outcomes"] = {"a": RuntimeError("s3 down")}

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup()

    assert state["ready"] is False
    # Publication already happened — only readiness is withheld.
    assert "publish" in calls
    assert "mark_ready" not in calls
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


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
    assert state["prewarm_urls"] == ["a", "b"]


@pytest.mark.asyncio
async def test_catalogue_is_loaded_off_the_event_loop(warmup_env, monkeypatch):
    loop_thread = threading.current_thread()
    seen = []
    monkeypatch.setattr(
        startup,
        "prewarm_stores",
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
