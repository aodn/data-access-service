import logging
from pathlib import Path

import yaml
import boto3
import os
import tempfile
from dotenv import load_dotenv

from data_access_service.config.env_type import EnvType


class Config:
    DEBUG = True
    LOGLEVEL = logging.DEBUG
    BASE_URL = "/api/v1/das"

    def __init__(self):
        load_dotenv()
        self.config = None
        self.s3 = boto3.client("s3")

    @staticmethod
    def load_config(file_path: str):
        current_dir = Path(__file__)
        resolved_file_path = current_dir.resolve().parent.parent.parent / file_path
        with open(resolved_file_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    @staticmethod
    def get_config(profile: EnvType = None):
        if profile is None:
            profile = EnvType(os.getenv("PROFILE", EnvType.DEV))

        print(f"Env profile is : {profile}")

        match profile:
            case EnvType.PRODUCTION:
                return ProdConfig()

            case EnvType.EDGE:
                return EdgeConfig()

            case EnvType.STAGING:
                return StagingConfig()

            case EnvType.TESTING:
                return IntTestConfig()

            case _:
                return DevConfig()

    @staticmethod
    def get_temp_folder(job_id: str) -> str:
        return tempfile.mkdtemp(prefix=job_id)

    def get_s3_client(self):
        return self.s3

    def get_csv_bucket_name(self):
        return (
            self.config["aws"]["s3"]["bucket_name"]["csv"]
            if self.config is not None
            else None
        )

    def get_job_queue_name(self):
        return (
            self.config["aws"]["batch"]["job_queue"]
            if self.config is not None
            else None
        )

    def get_job_definition_name(self):
        return (
            self.config["aws"]["batch"]["job_definition"]
            if self.config is not None
            else None
        )

    def get_compute_environment_name(self):
        return (
            self.config["aws"]["batch"]["compute_environment"]
            if self.config is not None
            else None
        )

    def get_job_definition_distinct_fields(self):
        """
        Returns the distinct fields that should be ignored when comparing job definitions.
        This is used to determine if a job definition needs to be updated.
        """
        return self.config["aws"]["batch"].get("job_definition_distinct_fields", [])

    @staticmethod
    def get_s3_temp_folder_name(master_job_id: str):
        """
        Returns the temporary folder (to place all the data file to zip later) name for a given master job ID.
        :param master_job_id: The ID of the master job, which is the init job ID.
        """
        return f"{master_job_id}/temp/"

    @staticmethod
    def get_month_count_per_job():
        """
        Returns the number of months to process in each job.
        This is used to split the date range into smaller chunks for processing.
        """
        return 12

    def get_sender_email(self):
        return (
            self.config["aws"]["ses"]["sender_email"]
            if self.config is not None
            else None
        )

    def get_api_key(self):
        return os.getenv("API_KEY")


class IntTestConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("tests/config/config-test.yaml")

    def set_s3_client(self, s3_client):
        self.s3 = s3_client

    def get_api_key(self):
        return "testing"


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
        self.config = Config.load_config(
            "data_access_service/config/config-staging.yaml"
        )


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-prod.yaml")
