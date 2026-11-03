from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OriginalNetcdf:
    """One original NetCDF file that matches the user's request."""

    bucket: str
    key: str
    time_coverage_start: datetime
    time_coverage_end: datetime
    lat_min: float | None
    lat_max: float | None
    lon_min: float | None
    lon_max: float | None


@dataclass(frozen=True)
class DataIndexConfig:
    """S3 Tables catalog the data-index DuckDB client attaches.

    ``table_bucket_arn`` is the bucket ARN from the S3 Tables console
    (``arn:aws:s3tables:region:account:bucket/name``). DuckDB's iceberg
    extension attaches that ARN with ``ENDPOINT_TYPE s3_tables``.
    """

    region: str
    table_bucket: str
    namespace: str
    table_name: str
    table_bucket_arn: str
    catalog_name: str
    memory_limit: str
    threads: int
