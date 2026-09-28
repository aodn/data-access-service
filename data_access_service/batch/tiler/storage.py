"""Write the batch's files to S3."""

import io
import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.utils.s3_json import split_s3


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def write_json(aws: AWSHelper, path: str, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON to ``path``."""
    bucket, key = split_s3(path)
    aws.upload_fileobj_to_s3(io.BytesIO(json.dumps(data).encode()), bucket, key)


def upload_file(aws: AWSHelper, local_path: str, path: str) -> None:
    """Upload the file at ``local_path`` to ``path``."""
    bucket, key = split_s3(path)
    aws.upload_file_to_s3(local_path, bucket, key)
