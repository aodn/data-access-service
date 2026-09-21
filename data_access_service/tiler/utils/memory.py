import ctypes
import logging
import sys
import threading
import time

import psutil

from data_access_service.config.config import Config

logger = logging.getLogger(__name__)

_MB = 1024 * 1024


_MIN_TRIM_INTERVAL_SECONDS = 2.0


try:
    _libc = ctypes.CDLL("libc.so.6") if sys.platform == "linux" else None
except OSError:
    _libc = None


_THRESHOLD_MB = Config.get_config().get_tiler_api_config().trim_threshold_mb

_process = psutil.Process()
_trim_lock = threading.Lock()
_last_trim = 0.0


def _malloc_trim() -> bool:
    """Ask glibc to return freed heap memory to the OS. False (no-op) where
    malloc_trim isn't available, such as macOS dev machines."""
    if _libc is None:
        return False
    return bool(_libc.malloc_trim(0))


def trim_if_over_threshold() -> None:
    global _last_trim
    if _libc is None:
        return
    if time.monotonic() - _last_trim < _MIN_TRIM_INTERVAL_SECONDS:
        return
    if _process.memory_info().rss / _MB < _THRESHOLD_MB:
        return
    # One thread trims; the rest get on with their tile.
    if not _trim_lock.acquire(blocking=False):
        return
    try:
        if time.monotonic() - _last_trim < _MIN_TRIM_INTERVAL_SECONDS:
            return
        before = _process.memory_info().rss / _MB
        started = time.monotonic()
        _malloc_trim()
        _last_trim = time.monotonic()
        logger.info(
            "malloc_trim: RSS %.0fMB -> %.0fMB in %.0fms",
            before,
            _process.memory_info().rss / _MB,
            (_last_trim - started) * 1000,
        )
    finally:
        _trim_lock.release()
