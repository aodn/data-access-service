import logging
import yaml
import boto3
import os
import tempfile
import logging.config

from threading import Lock
from pathlib import Path
from enum import Enum
from botocore.client import BaseClient
from dotenv import load_dotenv


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

    # Singleton storage and lock
    _instances = {}  # Dictionary to store single instances per EnvType
    _lock = Lock()  # Thread-safe lock for singleton initialization

    def __init__(self):
        load_dotenv()
        self.config = None
        self.s3 = None
        self.ses = None
        self.batch = None

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

        # Use lock to ensure thread-safe singleton instantiation
        with Config._lock:
            # Check if instance exists for the given profile
            if profile not in Config._instances:
                match profile:
                    case EnvType.PRODUCTION:
                        Config._instances[profile] = ProdConfig()
                    case EnvType.EDGE:
                        Config._instances[profile] = EdgeConfig()
                    case EnvType.STAGING:
                        Config._instances[profile] = StagingConfig()
                    case EnvType.TESTING:
                        Config._instances[profile] = IntTestConfig()
                    case _:
                        Config._instances[profile] = DevConfig()
            return Config._instances[profile]

    @staticmethod
    def get_temp_folder(job_id: str) -> str:
        return tempfile.mkdtemp(suffix=job_id)

    def get_s3_client(self) -> BaseClient:
        return self.s3

    def get_ses_client(self) -> BaseClient:
        return self.ses

    def get_batch_client(self) -> BaseClient:
        return self.batch

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

    def set_ses_client(self, ses_client):
        self.ses = ses_client

    def get_api_key(self):
        return "testing"

    @staticmethod
    def get_s3_test_key():
        return "test"

    @staticmethod
    def get_s3_secret():
        return "test"

    @staticmethod
    def get_temp_folder(job_id: str) -> str:
        return f"/tmp/tmp{job_id}"


class DevConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-dev.yaml")
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")


class EdgeConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-edge.yaml")
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")


class StagingConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_config(
            "data_access_service/config/config-staging.yaml"
        )
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_config("data_access_service/config/config-prod.yaml")
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")
