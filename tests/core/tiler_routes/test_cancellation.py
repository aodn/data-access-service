"""run_cancellable — the coordinator behind issue #9195's cancelled-tile handling.

Pins the two cases the ticket cares about: work still queued for a thread-pool
slot never starts once the client is gone, and work already running on a
worker thread is not waited on (but does keep running to completion in the
background — Deduper-shared work must not be aborted for other live callers).
"""

import threading
import time

import pytest

from data_access_service.config.config import Config
from data_access_service.core.tiler_routes import shared
from data_access_service.core.tiler_routes.shared import (
    TILE_THREAD_LIMITER,
    ClientDisconnected,
    run_cancellable,
)


def test_tile_thread_limiter_sized_from_config():
    """run_cancellable (and every async tiler handler that dispatches directly)
    must draw on a budget sized by tiler.thread_pool_size, not anyio's ambient
    process-wide default."""
    assert (
        TILE_THREAD_LIMITER.total_tokens
        == Config.get_config().get_tiler_api_config().thread_pool_size
    )


class _FakeRequest:
    """A minimal stand-in for fastapi.Request exposing only is_disconnected()."""

    def __init__(self, disconnected_fn):
        self._disconnected_fn = disconnected_fn

    async def is_disconnected(self) -> bool:
        return self._disconnected_fn()


@pytest.fixture(autouse=True)
def fast_poll(monkeypatch):
    """Tighten the disconnect-poll interval so tests don't wait on the
    production 0.1s cadence."""
    monkeypatch.setattr(shared, "_DISCONNECT_POLL_INTERVAL", 0.01)


@pytest.mark.asyncio
async def test_normal_completion_returns_fn_result():
    request = _FakeRequest(lambda: False)
    result = await run_cancellable(request, lambda: 42)
    assert result == 42


@pytest.mark.asyncio
async def test_disconnect_before_dispatch_means_fn_never_runs():
    """Cancelled while still queued for a thread-pool slot: fn must not start."""
    started = threading.Event()

    def fn():
        started.set()
        return "should not happen"

    request = _FakeRequest(lambda: True)  # already disconnected
    with pytest.raises(ClientDisconnected):
        await run_cancellable(request, fn)

    # Give a stray dispatch a chance to prove itself wrong before asserting.
    assert not started.wait(timeout=0.05)


@pytest.mark.asyncio
async def test_disconnect_after_start_returns_early_but_thread_keeps_running():
    """Cancelled after work has started on a worker thread: the coordinator
    stops waiting immediately, but the thread is not aborted — it keeps
    running to completion (e.g. because Deduper has other live waiters)."""
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def fn():
        started.set()
        release.wait(timeout=5)
        finished.set()
        return "done"

    # Reports "connected" until fn has actually started, then "disconnected".
    request = _FakeRequest(started.is_set)

    t0 = time.monotonic()
    with pytest.raises(ClientDisconnected):
        await run_cancellable(request, fn)
    elapsed = time.monotonic() - t0

    assert started.is_set()
    assert not finished.is_set()  # still blocked on release, we didn't wait for it
    assert elapsed < 1.0  # returned promptly, not after the thread's own work

    release.set()
    assert finished.wait(timeout=5)


@pytest.mark.asyncio
async def test_exception_from_fn_propagates_unwrapped():
    """anyio task groups wrap child exceptions in an ExceptionGroup; callers
    (route handlers catching ValueError/HTTPException) must still see the
    original exception type."""
    request = _FakeRequest(lambda: False)

    def boom():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await run_cancellable(request, boom)
