"""tiler.utils.memory: malloc_trim(0) before a tile when RSS crosses
trim_threshold_mb, so a burst's freed heap goes back to the OS while the burst is
still running.

Covers: only trims above the threshold, only one thread trims at a time,
back-to-back calls are rate limited, and malloc_trim safely no-ops where
libc.so.6 isn't available.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from data_access_service.tiler.utils import memory as mw


@pytest.fixture(autouse=True)
def _fresh_module_state(monkeypatch):
    """Each test starts with no recent trim."""
    monkeypatch.setattr(mw, "_last_trim", 0.0)
    monkeypatch.setattr(mw, "_trim_lock", threading.Lock())
    monkeypatch.setattr(mw, "_libc", MagicMock())


def _stub_config(monkeypatch, threshold_mb=2048):
    monkeypatch.setattr(mw, "_THRESHOLD_MB", threshold_mb)


def _fake_rss(monkeypatch, mb):
    process = MagicMock()
    process.memory_info.return_value = SimpleNamespace(rss=mb * 1024 * 1024)
    monkeypatch.setattr(mw, "_process", process)


def _spy_trim(monkeypatch):
    trim = MagicMock(return_value=True)
    monkeypatch.setattr(mw, "_malloc_trim", trim)
    return trim


class TestThreshold:
    def test_does_not_trim_below_threshold(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 1000)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()

        trim.assert_not_called()

    def test_trims_above_threshold(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 3000)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()

        trim.assert_called_once()

    def test_rss_exactly_at_threshold_still_trims(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 2048)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()

        trim.assert_called_once()

    def test_an_out_of_reach_threshold_turns_trimming_off(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=10**9)
        _fake_rss(monkeypatch, 9999)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()

        trim.assert_not_called()

    def test_no_libc_never_trims(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=1)
        _fake_rss(monkeypatch, 9999)
        trim = _spy_trim(monkeypatch)
        monkeypatch.setattr(mw, "_libc", None)

        mw.trim_if_over_threshold()

        trim.assert_not_called()


class TestRateLimit:
    def test_a_second_call_straight_after_does_not_trim_again(self, monkeypatch):
        """Nothing new has been freed in between, so the second trim would
        walk the whole heap for nothing."""
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 3000)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()
        mw.trim_if_over_threshold()

        trim.assert_called_once()

    def test_trims_again_once_the_interval_has_passed(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 3000)
        trim = _spy_trim(monkeypatch)

        mw.trim_if_over_threshold()
        monkeypatch.setattr(mw, "_last_trim", 0.0)
        mw.trim_if_over_threshold()

        assert trim.call_count == 2


class TestOneThreadAtATime:
    def test_a_tile_whose_trim_is_already_running_does_not_wait(self, monkeypatch):
        """The lock is taken without blocking, so the other tiles in flight
        carry on instead of queueing behind the trim."""
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 3000)
        trim = _spy_trim(monkeypatch)
        held = threading.Lock()
        held.acquire()
        monkeypatch.setattr(mw, "_trim_lock", held)

        mw.trim_if_over_threshold()

        trim.assert_not_called()

    def test_the_lock_is_released_even_if_trimming_raises(self, monkeypatch):
        _stub_config(monkeypatch, threshold_mb=2048)
        _fake_rss(monkeypatch, 3000)
        monkeypatch.setattr(mw, "_malloc_trim", MagicMock(side_effect=OSError("boom")))

        with pytest.raises(OSError):
            mw.trim_if_over_threshold()

        assert not mw._trim_lock.locked()


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
