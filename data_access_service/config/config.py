import logging
from enum import Enum

import yaml


class EnvType(Enum):
    DEV = "dev"
    EDGE = "edge"
    STAGING = "staging"
    PRODUCTION = "prod"


class Config:
    DEBUG = True
    LOGLEVEL = logging.DEBUG
    BASE_URL = "/api/v1/das"


class DevConfig(Config):
    pass


class EdgeConfig(Config):
    pass


class StagingConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO


def load_config(file_path="config.yaml"):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config
