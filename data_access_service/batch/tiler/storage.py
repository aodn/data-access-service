"""Write the batch's files to S3."""

import io
import json
from typing import Any

from boto3.s3.transfer import TransferConfig

from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.utils.s3_json import split_s3


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def write_json(aws: AWSHelper, path: str, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON to ``path``."""
    bucket, key = split_s3(path)
    aws.upload_fileobj_to_s3(io.BytesIO(json.dumps(data).encode()), bucket, key)


# One connection per upload: files are uploaded in parallel instead, and
# boto3's client allows 10 connections.
_UPLOAD_CONFIG = TransferConfig(use_threads=False)


def upload_file(aws: AWSHelper, local_path: str, path: str) -> None:
    """Upload the file at ``local_path`` to ``path``."""
    bucket, key = split_s3(path)
    aws.s3.upload_file(local_path, bucket, key, Config=_UPLOAD_CONFIG)
