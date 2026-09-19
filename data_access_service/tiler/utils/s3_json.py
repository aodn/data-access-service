"""Read the batch's JSON files from S3."""

import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper


def _split_s3(s3_url: str) -> tuple[str, str]:
    bucket, _, key = s3_url.removeprefix("s3://").partition("/")
    return bucket, key


def read_json(path: str) -> dict[str, Any]:
    """The JSON at ``path``. Raises FileNotFoundError if missing."""
    bucket, key = _split_s3(path)
    body = AWSHelper().get_s3_object(bucket, key)
    if body is None:
        raise FileNotFoundError(f"{path!r} not found")
    return json.loads(body)
