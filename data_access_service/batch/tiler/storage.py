"""S3 storage for batch-generated parquet + JSON sidecars.

``TilerParquetConfig.output_dir`` is always an ``s3://`` URI (see
``Config.get_tiler_parquet_config``). Paths join with a plain ``/``, not
``os.path.join`` (which mangles an ``s3://`` prefix).
"""

import io
import json
from typing import Any

from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclient import DuckDBClient


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


class _AdHocDuckDBClient(DuckDBClient):
    """Wraps an existing connection just to reuse ``create_s3_secret``'s
    boto3-credential logic — not a lifecycle-managing client of its own."""

    def __init__(self, con) -> None:
        self._con = con

    def get_instance(self):
        return self._con

    def execute(self, sql, params=None):
        return self._con.execute(sql, params or [])

    def close(self) -> None:
        pass


def configure_s3(con, s3_path: str) -> None:
    """Load httpfs and create an S3 secret for ``s3_path``'s bucket, so
    ``con`` can ``COPY ... TO`` directly into it."""
    bucket, _ = _split_s3(s3_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    _AdHocDuckDBClient(con).create_s3_secret(bucket)
