"""Write the batch's JSON files to S3."""

import io
import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.utils.s3_json import split_s3


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def write_json(path: str, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON to ``path``."""
    bucket, key = split_s3(path)
    AWSHelper().upload_fileobj_to_s3(io.BytesIO(json.dumps(data).encode()), bucket, key)
