import os
import resource

import psutil


def log_memory_usage(logger, checkpoint: str) -> None:
    """Log current and peak RSS of this process at a named checkpoint,
    so memory investigations can read the app log instead of CloudWatch metrics."""
    rss_gb = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
    # ru_maxrss is in KB on Linux (bytes on macOS; batch jobs run on Linux)
    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024**2)
    logger.info(f"[memory] {checkpoint}: rss={rss_gb:.2f} GB, peak={peak_gb:.2f} GB")
