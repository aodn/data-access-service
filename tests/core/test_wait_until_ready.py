"""API.wait_until_ready: the indefinite-wait branch and the scheduler's default.

The method is shared between core/scheduler.py, which wants a bounded wait and
ignores the result, and tiler warmup, which cannot derive anything useful from a
half-populated metadata index and so waits forever. Both behaviours have to
coexist in one method.
"""

import asyncio

import pytest

from data_access_service.core.api import BaseAPI


class _Api(BaseAPI):
    """Becomes ready after ``ready_after`` polls; never ready if None."""

    def __init__(self, ready_after=None):
        self._polls = 0
        self._ready_after = ready_after

    def get_api_status(self) -> bool:
        self._polls += 1
        if self._ready_after is None:
            return False
        return self._polls > self._ready_after


@pytest.fixture(autouse=True)
def no_real_sleep(monkeypatch):
    """The loop sleeps 0.5s per poll; tests only care about the poll count."""

    async def instant(_seconds):
        return None

    monkeypatch.setattr(asyncio, "sleep", instant)


@pytest.mark.asyncio
async def test_returns_true_once_ready():
    assert await _Api(ready_after=3).wait_until_ready(timeout=10) is True


@pytest.mark.asyncio
async def test_returns_false_on_timeout():
    """The caller that cannot proceed without metadata needs to tell the
    difference; the previous version only logged a warning and returned None."""
    assert await _Api().wait_until_ready(timeout=1) is False


@pytest.mark.asyncio
async def test_timeout_none_does_not_raise_on_the_comparison():
    """`waited >= timeout` raises TypeError against None, so the indefinite
    case needs its own branch rather than falling through the same comparison."""
    assert await _Api(ready_after=5).wait_until_ready(timeout=None) is True


@pytest.mark.asyncio
async def test_timeout_none_keeps_waiting_past_the_old_default():
    api = _Api(ready_after=1000)  # 500s of simulated waiting, past the 300s default
    assert await api.wait_until_ready(timeout=None) is True


@pytest.mark.asyncio
async def test_indefinite_wait_logs_progress_periodically(caplog):
    with caplog.at_level("INFO"):
        await _Api(ready_after=200).wait_until_ready(timeout=None)

    progress = [
        r for r in caplog.records if "Still waiting for API metadata" in r.message
    ]
    # 200 polls is 100 simulated seconds — one report at the 60s mark.
    assert len(progress) == 1


@pytest.mark.asyncio
async def test_default_timeout_is_still_300_seconds():
    """core/scheduler.py calls this with no arguments and ignores the result;
    its behaviour must be untouched by the tiler's needs."""
    api = _Api()
    assert await api.wait_until_ready() is False
    # 300s at 0.5s per poll, plus the initial check before the first sleep.
    assert api._polls == 601


@pytest.mark.asyncio
async def test_already_ready_returns_without_waiting():
    api = _Api(ready_after=0)
    assert await api.wait_until_ready() is True
    assert api._polls == 2  # loop condition, then the closing status log
