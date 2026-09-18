"""S3 reads for the tiler's batch-published JSON (root_metadata.json +
each store's metadata.json sidecar).
"""

import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper


def _split_s3(s3_url: str) -> tuple[str, str]:
    bucket, _, key = s3_url.removeprefix("s3://").partition("/")
    return bucket, key


def read_json(path: str) -> dict[str, Any]:
    """The parsed JSON object at ``path`` (an ``s3://`` URI).

    Raises FileNotFoundError if it doesn't exist, matching what ``open()``
    would raise for a missing local file.
    """
    bucket, key = _split_s3(path)
    body = AWSHelper().get_s3_object(bucket, key)
    if body is None:
        raise FileNotFoundError(f"{path!r} not found")
    return json.loads(body)
