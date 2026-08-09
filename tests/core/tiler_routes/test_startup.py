"""Tiler warmup sequencing and readiness.

The shape being defended: nothing is published before it is verified, and every
fatal path leaves the tiler unready rather than serving a catalogue that is
quietly wrong. No product is privileged here — that the original five ids still
derive correctly is pinned against the derivation formula in test_discovery.
"""

import asyncio

import pytest

from data_access_service.core.tiler_routes import shared, startup
from data_access_service.core.tiler_routes.startup import run_tiler_warmup
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.verification import VerificationResult

# --- warmup sequencing ------------------------------------------------------


class FakeAPI:
    def __init__(self, ready=True, index=None):
        self._ready = ready
        self._index = index if index is not None else {"u1": {}}
        self.wait_timeouts: list[float | None] = []

    async def wait_until_ready(self, timeout=300):
        self.wait_timeouts.append(timeout)
        return self._ready

    def get_dataset_variables(self, uuid=None):
        return self._index


@pytest.fixture
def warmup_env(monkeypatch):
    """Stub every step around discovery so ordering can be observed directly."""
    calls: list[str] = []
    state = {
        "candidates": {
            "a:v": Product(id="a:v", source_path="s3://b/a.zarr", variable="v")
        },
        "outcomes": {"s3://b/a.zarr": None},
        "result": None,
        "published": None,
        "ready": False,
    }
    state["result"] = VerificationResult(products=dict(state["candidates"]))

    def record(name, value=None):
        def _fn(*args, **kwargs):
            calls.append(name)
            return value

        return _fn

    async def fake_prewarm(urls):
        calls.append("prewarm")
        state["prewarm_urls"] = urls
        return state["outcomes"]

    def fake_verify(candidates, outcomes):
        calls.append("verify")
        return state["result"]

    def fake_publish(products):
        calls.append("publish")
        state["published"] = products

    def fake_mark_ready():
        calls.append("mark_ready")
        state["ready"] = True

    monkeypatch.setattr(startup, "load_gridded_variables", record("load_config", []))
    monkeypatch.setattr(
        startup,
        "build_candidate_products",
        lambda *a, **k: (calls.append("discover"), state["candidates"])[1],
    )
    monkeypatch.setattr(startup, "log_unmatched_overrides", record("overrides"))
    monkeypatch.setattr(startup, "load_colormaps", record("colormaps"))
    monkeypatch.setattr(startup, "warmup_resample", record("resample"))
    monkeypatch.setattr(startup, "warmup_visual", record("visual"))
    monkeypatch.setattr(startup, "prewarm_stores", fake_prewarm)
    monkeypatch.setattr(startup, "verify_candidate_products", fake_verify)
    monkeypatch.setattr(startup, "publish_products", fake_publish)
    monkeypatch.setattr(startup, "mark_tiler_ready", fake_mark_ready)

    return calls, state


@pytest.mark.asyncio
async def test_happy_path_publishes_then_marks_ready(warmup_env):
    calls, state = warmup_env
    await run_tiler_warmup(FakeAPI())

    assert state["ready"] is True
    assert state["published"] == state["result"].products
    # Verification precedes publication precedes readiness.
    assert calls.index("prewarm") < calls.index("verify")
    assert calls.index("verify") < calls.index("publish")
    assert calls.index("publish") < calls.index("mark_ready")


@pytest.mark.asyncio
async def test_warmup_waits_indefinitely_for_metadata(warmup_env):
    api = FakeAPI()
    await run_tiler_warmup(api)
    assert api.wait_timeouts == [None]


@pytest.mark.asyncio
async def test_unready_api_leaves_the_tiler_unready(warmup_env, caplog):
    calls, state = warmup_env

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI(ready=False))

    assert state["ready"] is False
    assert "discover" not in calls
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


@pytest.mark.asyncio
async def test_prewarm_receives_every_unique_candidate_source_path(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", source_path="s3://b/a.zarr", variable="v"),
        "a:w": Product(id="a:w", source_path="s3://b/a.zarr", variable="w"),
        "b:v": Product(id="b:v", source_path="s3://b/b.zarr", variable="v"),
    }
    state["result"] = VerificationResult(products=dict(state["candidates"]))
    state["outcomes"] = {"s3://b/a.zarr": None, "s3://b/b.zarr": None}

    await run_tiler_warmup(FakeAPI())

    # Deduplicated and sorted — 85 products but only 60 opens.
    assert state["prewarm_urls"] == ["s3://b/a.zarr", "s3://b/b.zarr"]


@pytest.mark.asyncio
async def test_verification_dropping_everything_leaves_the_tiler_unready(
    warmup_env, caplog
):
    calls, state = warmup_env
    state["result"] = VerificationResult(products={})

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI())

    assert state["ready"] is False
    assert state["published"] is None
    assert "publish" not in calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing_step",
    [
        "load_gridded_variables",
        "build_candidate_products",
        "log_unmatched_overrides",
        "publish_products",
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
        await run_tiler_warmup(FakeAPI())

    assert state["ready"] is False
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_is_re_raised_not_logged_as_failure(
    warmup_env, monkeypatch, caplog
):
    """Warmup runs as a lifespan task whose result is never awaited. Swallowing
    CancelledError would turn every shutdown into a spurious CRITICAL."""
    calls, state = warmup_env

    async def cancelled(urls):
        raise asyncio.CancelledError()

    monkeypatch.setattr(startup, "prewarm_stores", cancelled)

    with caplog.at_level("CRITICAL"):
        with pytest.raises(asyncio.CancelledError):
            await run_tiler_warmup(FakeAPI())

    assert not any("Tiler warmup failed" in r.message for r in caplog.records)


@pytest.fixture(autouse=True)
def restore_tiler_readiness():
    """run_tiler_warmup flips module-level readiness; put it back afterwards."""
    saved = shared._tiler_ready
    yield
    shared._tiler_ready = saved
