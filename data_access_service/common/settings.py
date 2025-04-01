import logging
from enum import Enum

import yaml


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


def load_config(file_path="data_access_service/config/config.yaml"):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config
