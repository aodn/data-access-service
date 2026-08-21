import logging
import os
import time


def set_process_timezone_utc() -> None:
    """Force UTC as the process timezone, so "local time" is always UTC.

    Python asks the C library, which reads TZ. Without this, naive calls like
    datetime.now() and pd.Timestamp.today() follow the host clock, so an AWS
    host outside UTC can resolve "today" to the wrong date.
    """
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):  # POSIX only; absent on Windows
        time.tzset()


# Run on import so every entry point (server, AWS Batch entry_point, pytest)
# gets it before any module reads the clock.
set_process_timezone_utc()

from data_access_service.config.config import Config
from data_access_service.core.api import API


def init_log(config: Config):
    logging.basicConfig(
        level=config.LOGLEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # If add new logger setting, please put in alphabetical order
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("numcodecs").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("rasterio").setLevel(logging.WARNING)
    logging.getLogger("s3fs").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)

    # No need to see info logs from aodn package in edge, staging or production
    if os.getenv("PROFILE") not in (None, "dev", "testing"):
        logging.getLogger("aodn").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    return logger
