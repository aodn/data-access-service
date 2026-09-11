"""memory_watchdog: forces gc.collect() + malloc_trim(0) when RSS crosses
threshold_mb (see backlog#9208 — xarray/Zarr objects form reference cycles
that refcounting alone can't free).

Covers: only triggers above threshold, never swallows cancellation (the
lifespan cancels this task on shutdown), and malloc_trim safely no-ops where
libc.so.6 isn't available.
"""

import asyncio
import gc
import weakref
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from data_access_service.config.config import Config
from data_access_service.core import memory_watchdog as mw


def _config(enabled=True, interval_seconds=60, threshold_mb=2048):
    return SimpleNamespace(
        enabled=enabled, interval_seconds=interval_seconds, threshold_mb=threshold_mb
    )


def _stub_config(monkeypatch, **kwargs):
    stub = MagicMock()
    stub.get_memory_watchdog_config.return_value = _config(**kwargs)
    monkeypatch.setattr(Config, "get_config", lambda *a, **k: stub)


def _fake_process(monkeypatch, rss_sequence_mb):
    """process.memory_info().rss yields each value in sequence (MB, converted
    to bytes); the last value repeats once the sequence is exhausted."""
    values = iter(rss_sequence_mb)
    state = {"last": rss_sequence_mb[0]}

    def memory_info():
        state["last"] = next(values, state["last"])
        return SimpleNamespace(rss=state["last"] * 1024 * 1024)

    process = MagicMock()
    process.memory_info.side_effect = memory_info
    monkeypatch.setattr(mw.psutil, "Process", lambda: process)


def _one_tick_then_cancel(monkeypatch):
    """Let the loop body run exactly once, then stop it the same way the
    lifespan's shutdown does — by cancelling — rather than looping forever."""
    monkeypatch.setattr(
        mw.anyio, "sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
    )


class TestDisabled:
    @pytest.mark.asyncio
    async def test_returns_immediately_without_looping(self, monkeypatch):
        _stub_config(monkeypatch, enabled=False)
        sleep = AsyncMock()
        monkeypatch.setattr(mw.anyio, "sleep", sleep)

        await mw.run_memory_watchdog()

        sleep.assert_not_awaited()


class TestBelowThreshold:
    @pytest.mark.asyncio
    async def test_does_not_force_collection_below_threshold(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_process(monkeypatch, [1000])
        _one_tick_then_cancel(monkeypatch)
        collect = MagicMock()
        monkeypatch.setattr(mw.gc, "collect", collect)
        trim = MagicMock()
        monkeypatch.setattr(mw, "_malloc_trim", trim)

        with pytest.raises(asyncio.CancelledError):
            await mw.run_memory_watchdog()

        collect.assert_not_called()
        trim.assert_not_called()


class TestAboveThreshold:
    @pytest.mark.asyncio
    async def test_forces_gc_collect_and_malloc_trim_once_over_threshold(
        self, monkeypatch
    ):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_process(monkeypatch, [3000, 2000, 1500])
        _one_tick_then_cancel(monkeypatch)
        collect = MagicMock(return_value=42)
        monkeypatch.setattr(mw.gc, "collect", collect)
        trim = MagicMock(return_value=True)
        monkeypatch.setattr(mw, "_malloc_trim", trim)

        with pytest.raises(asyncio.CancelledError):
            await mw.run_memory_watchdog()

        collect.assert_called_once()
        trim.assert_called_once()

    @pytest.mark.asyncio
    async def test_rss_exactly_at_threshold_still_triggers(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_process(monkeypatch, [2048])
        _one_tick_then_cancel(monkeypatch)
        collect = MagicMock()
        monkeypatch.setattr(mw.gc, "collect", collect)
        monkeypatch.setattr(mw, "_malloc_trim", MagicMock())

        with pytest.raises(asyncio.CancelledError):
            await mw.run_memory_watchdog()

        collect.assert_called_once()


class TestCancellation:
    @pytest.mark.asyncio
    async def test_cancellation_during_sleep_propagates(self, monkeypatch):
        """Runs as a lifespan task the same way tiler_warmup does — swallowing
        CancelledError here would break shutdown for the whole app."""
        _stub_config(monkeypatch)
        monkeypatch.setattr(
            mw.anyio, "sleep", AsyncMock(side_effect=asyncio.CancelledError())
        )

        with pytest.raises(asyncio.CancelledError):
            await mw.run_memory_watchdog()


class TestMallocTrim:
    def test_returns_false_when_libc_unavailable(self, monkeypatch):
        monkeypatch.setattr(mw, "_libc", None)

        assert mw._malloc_trim() is False

    def test_calls_libc_malloc_trim_and_returns_bool_result(self, monkeypatch):
        libc = MagicMock()
        libc.malloc_trim.return_value = 1
        monkeypatch.setattr(mw, "_libc", libc)

        result = mw._malloc_trim()

        libc.malloc_trim.assert_called_once_with(0)
        assert result is True

    def test_zero_return_from_malloc_trim_is_falsy(self, monkeypatch):
        libc = MagicMock()
        libc.malloc_trim.return_value = 0
        monkeypatch.setattr(mw, "_libc", libc)

        assert mw._malloc_trim() is False


class _Node:
    """A plain object whose only purpose is to reference another one — two
    of these pointing at each other form a reference cycle."""

    def __init__(self):
        self.ref = None


class TestGcCollectReclaimsReferenceCycles:
    def test_refcounting_alone_leaves_a_cycle_alive_but_gc_collect_frees_it(self):
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            a = _Node()
            b = _Node()
            a.ref = b
            b.ref = a
            alive = weakref.ref(a)
            del a, b

            # No external reference remains, but refcounting alone can't
            # free a cycle — each half still holds the other's count above
            # zero.
            assert alive() is not None

            gc.collect()

            assert alive() is None
        finally:
            if was_enabled:
                gc.enable()
