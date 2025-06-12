import os

from data_access_service.config.config import EnvType


def sync_aws_batch_configs():
    profile = EnvType(os.getenv("PROFILE", EnvType.DEV))
