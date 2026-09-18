"""S3 storage for batch-generated parquet + JSON sidecars.

``TilerParquetConfig.output_dir`` is always an ``s3://`` URI (see
``Config.get_tiler_parquet_config``). Paths join with a plain ``/``, not
``os.path.join`` (which mangles an ``s3://`` prefix).
"""

import io
import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def _split_s3(s3_url: str) -> tuple[str, str]:
    bucket, _, key = s3_url.removeprefix("s3://").partition("/")
    return bucket, key


def read_json(path: str) -> dict[str, Any] | None:
    """The parsed JSON at ``path``, or None if it doesn't exist yet."""
    bucket, key = _split_s3(path)
    body = AWSHelper().get_s3_object(bucket, key)
    return json.loads(body) if body is not None else None


def write_json(path: str, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON to ``path`` on S3."""
    bucket, key = _split_s3(path)
    AWSHelper().upload_fileobj_to_s3(io.BytesIO(json.dumps(data).encode()), bucket, key)
