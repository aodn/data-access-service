import logging
from pathlib import Path

import yaml
import boto3
import os
import tempfile
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
        self._config = None
        self._size_for_mem_list = 1000000
        self._s3 = boto3.client("s3")

    @staticmethod
    def load_config(file_path: str):
        current_dir = Path(__file__)
        resolved_file_path = current_dir.resolve().parent.parent.parent / file_path
        with open(resolved_file_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    @staticmethod
    def get_config(profile: EnvType | None = None):
        if profile is None:
            profile = EnvType(os.getenv("PROFILE", EnvType.DEV))

        match profile:
            case EnvType.PRODUCTION:
                return ProdConfig()

            case EnvType.EDGE:
                return EdgeConfig()

            case EnvType.STAGING:
                return StagingConfig()

            case EnvType.TESTING:
                return TestConfig()

            case _:
                return DevConfig()

    @staticmethod
    def get_temp_folder(job_id: str) -> str:
        return tempfile.mkdtemp(prefix=job_id)

    # Set the max number of entry before we consider using file based backed list
    def get_max_size_for_mem_list(self):
        return self._size_for_mem_list

    def get_s3_client(self):
        return self._s3

    def get_csv_bucket_name(self):
        return (
            self._config["aws"]["s3"]["bucket_name"]["csv"]
            if self._config is not None
            else None
        )

    def get_sender_email(self):
        return (
            self._config["aws"]["ses"]["sender_email"]
            if self._config is not None
            else None
        )


class TestConfig(Config):
    def __init__(self):
        super().__init__()
        self._config = Config.load_config("tests/config/config-test.yaml")

    def set_s3_client(self, s3_client):
        self._s3 = s3_client


class DevConfig(Config):
    def __init__(self):
        super().__init__()
        self._config = Config.load_config("data_access_service/config/config-dev.yaml")


class EdgeConfig(Config):
    def __init__(self):
        super().__init__()
        self._config = Config.load_config("data_access_service/config/config-edge.yaml")


class StagingConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self._config = Config.load_config(
            "data_access_service/config/config-staging.yaml"
        )


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self._config = Config.load_config("data_access_service/config/config-prod.yaml")
