import logging
from enum import Enum


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
