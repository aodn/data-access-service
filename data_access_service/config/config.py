from typing import List
import yaml
import boto3
import os
import tempfile
import logging.config

from threading import Lock
from pathlib import Path
from enum import Enum
from typing import Dict
from botocore.client import BaseClient
from dotenv import load_dotenv
from data_access_service.models.pmtiles_types import (
    PmtilesGenerationConfig,
    HexLayerSpec,
)


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
        self.duckdb_maxmem = None

    @staticmethod
    def load_config(file_path: str):
        current_dir = Path(__file__)
        resolved_file_path = current_dir.resolve().parent.parent.parent / file_path
        with open(resolved_file_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    @staticmethod
    def _deep_merge(base: Dict, override: Dict) -> Dict:
        """Deep merge override dict into a copy of base. Nested dicts are merged recursively; lists/scalars are replaced by override values."""
        if not isinstance(base, dict):
            base = {}
        if not isinstance(override, dict):
            override = {}
        result = dict(base)
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = Config._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    @staticmethod
    def load_merged_config(base_path: str, override_path: str):
        """Load base config then deep-merge the env/override config on top."""
        base = Config.load_config(base_path) or {}
        override = Config.load_config(override_path) or {}
        return Config._deep_merge(base, override)

    @staticmethod
    def get_config(profile: EnvType = None):
        if profile is None:
            if os.getenv("PYTEST_CURRENT_TEST") is not None:
                # User do not specify profile but running pytest so it must be testing
                profile = EnvType.TESTING
            else:
                profile = EnvType(os.getenv("PROFILE", EnvType.DEV))

        # Use lock to ensure thread-safe singleton instantiation
        with Config._lock:
            # Check if instance exists for the given profile
            if profile not in Config._instances:
                print(f"Env profile is : {profile}")
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
        if self.config is None:
            return None
        val = self.config["aws"]["s3"]["bucket_name"]["csv"]
        return val.strip() if isinstance(val, str) else val

    def get_duckdb_maxmem(self):
        if self.config is None:
            return "800M"
        return self.config.get("duckdb", {}).get("maxmem", "800M")

    def get_duckdb_threads(self):
        if self.config is None:
            return 8
        return self.config.get("duckdb", {}).get("threads", 8)

    def get_wave_buoy_backup_bucket_name(self):
        if self.config is None:
            return None
        val = self.config["aws"]["s3"]["bucket_name"]["wave_buoy_backup"]
        return val.strip() if isinstance(val, str) else val

    def get_mooring_backup_bucket_name(self):
        if self.config is None:
            return None
        val = self.config["aws"]["s3"]["bucket_name"]["mooring_backup"]
        return val.strip() if isinstance(val, str) else val

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

    def get_column_name_mapping(self) -> Dict[str, list[str]]:
        """
        Returns the ordered list of candidate column names for standard coordinate/time fields.
        The first candidate that exists in a dataset's metadata is used.

        Structure: { "TIME": ["TIME", "time", "JULD", ...], ... }
        """
        yaml_mapping = self.config.get("column_name_mapping")
        result = {}
        if yaml_mapping:
            # Merge: yaml values override/extend defaults per key, otherwise use default
            for key, candidates in yaml_mapping.items():
                if isinstance(candidates, list) or candidates is None:
                    result[key.casefold()] = candidates
        return result

    def get_sender_email(self):
        return (
            self.config["aws"]["ses"]["sender_email"]
            if self.config is not None
            else None
        )

    def get_api_key(self):
        return os.getenv("API_KEY")

    def get_pmtiles_config(self) -> PmtilesGenerationConfig:
        pmconfig = self.config.get("pmtiles", {}).get("config", {})
        return PmtilesGenerationConfig(
            output_pmtiles_dir=pmconfig["output_pmtiles_dir"],
            staged_parquet_dir=pmconfig["staged_parquet_dir"],
            geojsonseq_dir=pmconfig["geojsonseq_dir"],
            duckdb_temp_dir=pmconfig["duckdb_temp_dir"],
            duckdb_database=pmconfig["duckdb_database"],
            memory_limit=pmconfig["memory_limit"],
            threads=pmconfig["threads"],
            fetch_size=pmconfig["fetch_size"],
            bucket_name=pmconfig["bucket_name"],
        )

    def get_hex_layer_specs(self, dname: str) -> List[HexLayerSpec] | None:
        """
        Returns the list of hex layer specifications for a given dataset.
        :param dname: The name of the dataset.
        """
        if dname.endswith(".parquet"):
            dname = dname.removesuffix(".parquet")
            hex_layer_specs = self.config.get("pmtiles", {}).get("layers", {})

            specs = []
            for spec_name, spec_value in hex_layer_specs.items():
                specs.append(
                    HexLayerSpec(
                        name=spec_name,
                        h3_resolution=spec_value["h3_resolution"],
                        minzoom=spec_value["minzoom"],
                        maxzoom=spec_value["maxzoom"],
                        layer_geojsonseq_file_name=f"{dname}_{spec_name}.geojsonseq",
                    )
                )
            return specs
        else:
            return None


class IntTestConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_merged_config(
            "data_access_service/config/config.yaml",
            "tests/config/config-test.yaml",
        )

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
        self.config = Config.load_merged_config(
            "data_access_service/config/config.yaml",
            "data_access_service/config/config-dev.yaml",
        )
        self.batch = boto3.client("batch", region_name="ap-southeast-2")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses", region_name="ap-southeast-2")


class EdgeConfig(Config):
    def __init__(self):
        super().__init__()
        self.config = Config.load_merged_config(
            "data_access_service/config/config.yaml",
            "data_access_service/config/config-edge.yaml",
        )
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")


class StagingConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_merged_config(
            "data_access_service/config/config.yaml",
            "data_access_service/config/config-staging.yaml",
        )
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")


class ProdConfig(Config):
    DEBUG = False
    LOGLEVEL = logging.INFO

    def __init__(self):
        super().__init__()
        self.config = Config.load_merged_config(
            "data_access_service/config/config.yaml",
            "data_access_service/config/config-prod.yaml",
        )
        self.batch = boto3.client("batch")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")
