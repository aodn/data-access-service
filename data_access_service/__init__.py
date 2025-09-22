import logging
import os

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
    logging.getLogger("s3fs").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)

    # No need to see info logs from aodn package in edge, staging or production
    if os.getenv("PROFILE") not in (None, "dev", "testing"):
        logging.getLogger("aodn").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    return logger
