"""Read JSON files from S3."""

import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper


def split_s3(s3_url: str) -> tuple[str, str]:
    bucket, _, key = s3_url.removeprefix("s3://").partition("/")
    return bucket, key


def read_json(path: str, required: bool = True) -> dict[str, Any] | None:
    """The JSON at ``path``. If missing, raises FileNotFoundError when
    ``required``, otherwise returns None."""
    bucket, key = split_s3(path)
    body = AWSHelper().get_s3_object(bucket, key)
    if body is None:
        if required:
            raise FileNotFoundError(f"{path!r} not found")
        return None
    return json.loads(body)
