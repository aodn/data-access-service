import logging
import yaml
from enum import Enum

class EnvType(Enum):
    DEV = "dev"
    TESTING = "testing"
    EDGE = "edge"
    STAGING = "staging"
    PRODUCTION = "prod"


class Config:
    DEBUG = True
    LOGLEVEL = logging.DEBUG
    BASE_URL = "/api/v1/das"

    def __init__(self):
        self.config = None

    @staticmethod
    def load_config(file_path: str):
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    def get_csv_bucket_name(self):
        return self.config["aws"]["s3"]["bucket_name"]["csv"] if self.config is not None else None

    def get_sender_email(self):
        return self.config["aws"]["ses"]["sender_email"] if self.config is not None else None

class TestConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("tests/config/config-test.yaml")


class DevConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-dev.yaml")


class EdgeConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-edge.yaml")


class StagingConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-staging.yaml")


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-prod.yaml")

