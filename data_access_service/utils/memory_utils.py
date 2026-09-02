import os
import platform
import resource

import psutil

# ru_maxrss is in KB on Linux (a Linux-specific quirk) but bytes on others like macOS, 
# so we need to divide by 1024**2 on Linux and 1024**3 on others to get GB.
_RU_MAXRSS_BYTES_DIVISOR = 1024**2 if platform.system() == "Linux" else 1024**3


def log_memory_usage(logger, checkpoint: str) -> None:
    """Log current and peak RSS of this process at a named checkpoint,
    so memory investigations can read the app log instead of CloudWatch metrics."""
    rss_gb = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / _RU_MAXRSS_BYTES_DIVISOR
    logger.info(f"[memory] {checkpoint}: rss={rss_gb:.2f} GB, peak={peak_gb:.2f} GB")
