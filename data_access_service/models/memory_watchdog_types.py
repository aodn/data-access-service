from dataclasses import dataclass


@dataclass(frozen=True)
class MemoryWatchdogConfig:
    """Runtime tuning for the background memory watchdog (see
    core/memory_watchdog.py), read from the ``memory_watchdog:`` section of
    config.yaml (see Config.get_memory_watchdog_config()).
    """

    enabled: bool
    interval_seconds: int
    threshold_mb: int
