import logging

from data_access_service.config.config import Config
from data_access_service.core.api import API


def init_log(config: Config):
    logging.basicConfig(
        level=config.LOGLEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    return logger
