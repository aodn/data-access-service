"""Where a cloud optimised dataset's files actually live.

Most datasets sit in the AODN bucket, but some are hosted by another
organisation in its own bucket, behind its own endpoint and keys. Batch jobs
read the raw files with DuckDB rather than through DataQuery, so they need the
path and the credentials as plain values.

Deliberately imports no data source: every provider module imports this one to
describe itself, and :func:`co_data_registory.resolve_dataset_location` is what
puts the two together.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DatasetLocation:
    """One dataset's S3 home, and the credentials needed to read it."""

    bucket: str
    # Path of the dataset's parent folder inside the bucket: "" when the
    # dataset sits at the bucket root, otherwise a path ending with "/".
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
        """The s3 uri of the source parquet, not the http URL of s3 objects."""
        return f"s3://{self.bucket}/{self.prefix}{dataset_name}/**/*.parquet"
