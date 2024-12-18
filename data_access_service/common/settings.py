import logging
from enum import Enum


class Profile(Enum):
    DEV = "dev"
    EDGE = "edge"
    STAGING = "staging"
    PRODUCTION = "prod"


class Config:
    DEBUG = True
    LOGLEVEL = logging.DEBUG
    BASE_URL = "/api/v1/das"


class DevProfile(Config):
    pass


class EdgeProfile(Config):
    pass


class StagingProfile(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO


class ProdProfile(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO
