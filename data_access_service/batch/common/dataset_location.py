"""Where a cloud optimised dataset's parquet files actually live.

Most datasets sit in the AODN bucket, but some are hosted by another
organisation in its own bucket, behind its own endpoint and keys. Batch jobs
read the raw files with DuckDB rather than through DataQuery, so they need the
path and the credentials as plain values.
"""

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from aodn_cloud_optimised.lib.DataQuery import BUCKET_OPTIMISED_DEFAULT

from data_access_service.models.co_data_source.csiro_access import (
    get_csiro_key_request_url,
    request_csiro_s3_access,
)


@dataclass(frozen=True)
class DatasetLocation:
    """One dataset's S3 home, and the credentials needed to read it."""

    bucket: str
    # Path of the dataset's parent folder inside the bucket: "" for AODN,
    # otherwise a path ending with "/".
    prefix: str = ""
    # Host only, no scheme (what DuckDB's ENDPOINT wants). None means AWS S3.
    endpoint: Optional[str] = None
    use_ssl: bool = True
    access_key: Optional[str] = None
    secret_access_key: Optional[str] = None

    @property
    def is_external(self) -> bool:
        """True when the dataset needs its own keys, not the job's IAM role."""
        return self.access_key is not None

    def parquet_glob(self, dataset_name: str) -> str:
        """The s3 uri of the source parquet files."""
        return f"s3://{self.bucket}/{self.prefix}{dataset_name}/**/*.parquet"


def resolve_dataset_location(dataset_name: str) -> DatasetLocation:
    """Locate ``dataset_name``, requesting external keys when it is not AODN's.

    The keys are requested per call because they expire; a long job that
    resolved once at start-up could find them dead by the time it reads.
    """
    key_request_url = get_csiro_key_request_url(dataset_name)
    if key_request_url is None:
        return DatasetLocation(bucket=BUCKET_OPTIMISED_DEFAULT)

    access = request_csiro_s3_access(dataset_name, key_request_url)
    endpoint = urlparse(access.endpoint_url)
    return DatasetLocation(
        bucket=access.bucket,
        prefix=access.prefix,
        endpoint=endpoint.netloc or endpoint.path,
        use_ssl=endpoint.scheme != "http",
        access_key=access.access_key,
        secret_access_key=access.secret_access_key,
    )
