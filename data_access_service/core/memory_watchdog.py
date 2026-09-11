"""Background task that proactively frees xarray/Zarr reference-cycle
garbage left over from finished tile-request bursts.

xarray ``Dataset``/``DataArray``/``Variable`` objects built while serving a
tile request hold internal reference cycles, so CPython's refcounting alone
can never free them — only a generation-2 ``gc`` pass can (confirmed by
disabling ``gc`` entirely, serving a burst, dropping every reference, and
checking ``gc.get_objects()`` — see backlog#9208). Python only runs that pass
once its own allocation-count thresholds are crossed (``gc.get_threshold()``),
which needs ongoing allocation churn; once a burst of tile requests ends, the
only remaining activity is incidental background traffic (health checks,
etc.), so that trigger can take a long time to happen on its own.

``gc.collect()`` only frees Python objects (and, transitively, calls
``free()`` on the numpy/C buffers they own) — it doesn't make glibc hand that
freed heap space back to the OS, which is a separate allocator-level step.
``malloc_trim(0)`` asks glibc to do that. Measured on a real Linux/glibc
build of this service (production runs Linux; this doesn't apply on macOS
dev): a 3-burst sequence left RSS at 2601MB; ``gc.collect()`` alone dropped
it to 1655MB, and following it with ``malloc_trim(0)`` dropped it further to
1308MB — noticeably more recovery than either step alone.

This task closes the gap: on a fixed interval, it checks this process's RSS
and forces both steps once RSS crosses a configured threshold, instead of
waiting for enough incidental churn to trigger a gen2 pass naturally. It only
ever frees objects that are already unreachable — a still-in-flight
request's live objects are, by definition, still referenced, so a collection
pass can never touch them.
"""

import ctypes
import gc
import logging
import sys

import anyio
import psutil

from data_access_service.config.config import Config

logger = logging.getLogger(__name__)


try:
    _libc = ctypes.CDLL("libc.so.6") if sys.platform == "linux" else None
except OSError:
    _libc = None


def _malloc_trim() -> bool:
    """Ask glibc to return freed heap memory to the OS. False (no-op) where
    malloc_trim isn't available — gc.collect() still runs everywhere."""
    if _libc is None:
        return False
    return bool(_libc.malloc_trim(0))


async def run_memory_watchdog() -> None:
    """Check RSS every ``interval_seconds`` and force ``gc.collect()`` +
    ``malloc_trim(0)`` once it crosses ``threshold_mb``. Runs until cancelled
    (see server.py's lifespan, which cancels every background task on
    shutdown).
    """
    config = Config.get_config().get_memory_watchdog_config()
    if not config.enabled:
        logger.info("Memory watchdog disabled (memory_watchdog.config.enabled=false)")
        return

    process = psutil.Process()
    logger.info(
        "Memory watchdog started: checking every %ss, threshold %sMB",
        config.interval_seconds,
        config.threshold_mb,
    )
    while True:
        await anyio.sleep(config.interval_seconds)

        rss_mb = process.memory_info().rss / (1024 * 1024)
        if rss_mb < config.threshold_mb:
            continue

        logger.info(
            "Memory watchdog: RSS %.0fMB >= threshold %dMB, forcing gc.collect() "
            "+ malloc_trim(0)",
            rss_mb,
            config.threshold_mb,
        )
        collected = await anyio.to_thread.run_sync(gc.collect)
        rss_after_gc_mb = process.memory_info().rss / (1024 * 1024)
        trimmed = await anyio.to_thread.run_sync(_malloc_trim)
        rss_after_trim_mb = process.memory_info().rss / (1024 * 1024)
        logger.info(
            "Memory watchdog: gc.collect() freed %d objects (RSS %.0fMB -> "
            "%.0fMB); malloc_trim %s (RSS -> %.0fMB)",
            collected,
            rss_mb,
            rss_after_gc_mb,
            "returned memory" if trimmed else "unavailable or nothing to trim",
            rss_after_trim_mb,
        )
