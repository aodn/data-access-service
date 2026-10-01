"""Write the batch's files to S3."""

import io
import json
from typing import Any

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.utils.s3_json import split_s3


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def write_json(aws: AWSHelper, path: str, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON to ``path``."""
    bucket, key = split_s3(path)
    aws.upload_fileobj_to_s3(io.BytesIO(json.dumps(data).encode()), bucket, key)


class WriteConflict(Exception):
    """The object changed since it was read."""


def read_json_with_etag(
    aws: AWSHelper, path: str
) -> tuple[dict[str, Any] | None, str | None]:
    """The JSON at ``path`` and its ETag, or ``(None, None)`` if missing."""
    bucket, key = split_s3(path)
    try:
        response = aws.s3.get_object(Bucket=bucket, Key=key)
    except aws.s3.exceptions.NoSuchKey:
        return None, None
    return json.loads(response["Body"].read()), response["ETag"]


def write_json_if_unchanged(
    aws: AWSHelper, path: str, data: dict[str, Any], etag: str | None
) -> None:
    """Write ``data`` to ``path`` only if it still has ``etag`` (or still
    doesn't exist when ``etag`` is None). Raises WriteConflict otherwise."""
    bucket, key = split_s3(path)
    condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
    try:
        aws.s3.put_object(
            Bucket=bucket, Key=key, Body=json.dumps(data).encode(), **condition
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise WriteConflict(path) from e
        raise


# One connection per upload: files are uploaded in parallel instead, and
# boto3's client allows 10 connections.
_UPLOAD_CONFIG = TransferConfig(use_threads=False)


def upload_file(aws: AWSHelper, local_path: str, path: str) -> None:
    """Upload the file at ``local_path`` to ``path``."""
    bucket, key = split_s3(path)
    aws.s3.upload_file(local_path, bucket, key, Config=_UPLOAD_CONFIG)
