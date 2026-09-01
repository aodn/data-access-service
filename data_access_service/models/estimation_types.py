"""Types for the pre-built parquet estimation index.

The index is a small summary file, built by a batch job, that answers
"how many rows and how many bytes" for ``POST /data/{uuid}/estimate_size``
without scanning the real dataset on S3. One parquet holding
``(day, lat cell, lon cell) -> row count`` plus a JSON sidecar holding the
per-dataset numbers (bytes per CSV row, zip ratio, covered date range).
"""

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from data_access_service.models.duckdb_types import DuckDBTuningConfig

# Bumped when the parquet columns or the sidecar fields change in a way an
# older server cannot read. A server ignores an index whose version it does
# not know and falls back to the live scan.
ESTIMATION_INDEX_VERSION = 1

# Columns of the index parquet (see EstimationIndexBuilder).
INDEX_DAY_COLUMN = "d"
INDEX_LAT_BIN_COLUMN = "lat_bin"
INDEX_LON_BIN_COLUMN = "lon_bin"
INDEX_COUNT_COLUMN = "c"


@dataclass(frozen=True)
class EstimationIndexConfig:
    """Settings for building and reading the estimation index."""

    # S3 destination. Same bucket as pmtiles, sibling folder.
    bucket_name: str
    s3_prefix: str
    # Work-dir sub folder the index is written to before upload (relative path).
    output_dir: str

    # Grid cell size in degrees of the FIRST pass over the source data.
    bin_size: float
    # Whole-number factors to coarsen by when the index comes out too big.
    # Integers, so a coarser grid can be re-aggregated from the base one
    # (floor(floor(x/a)/n) == floor(x/(a*n)) only holds for integer n).
    bin_merge_factors: tuple[int, ...]
    max_index_rows: int

    # Pass B sampling: how many source files, and how many rows in total.
    sample_files: int
    sample_rows: int

    row_group_size: int

    # Master switch for the request side. False = always use the live scan.
    use_index_for_estimate: bool

    # DuckDB session settings for the build. Its own object, not the pmtiles
    # one, so the two jobs' memory limits move independently.
    duckdb: DuckDBTuningConfig
    # True: the batch run forks one child per dataset so DuckDB memory goes
    # back to the OS on exit. False: run in the main process (local debug).
    use_fork_process: bool
    # Source files scanned per chunk in pass A. DuckDB holds per-file state for
    # the life of the connection, so one scan over a 300k-file dataset needs
    # far more memory than the container has. 0 scans the whole dataset at once.
    chunk_files: int


@dataclass(frozen=True)
class EstimationSidecarMetadata:
    """JSON sidecar written beside the index parquet (``{key}.metadata``).

    Everything the request side needs that is one number per dataset, plus
    enough provenance to notice a stale index and fall back.
    """

    version: int
    uuid: str
    key: str
    # Cell size of the first pass. The cells actually in the file are
    # bin_size * bin_merge_factor wide - but never compute the bin from that
    # product: both sides must apply the same two floors in the same order,
    # or they disagree by a cell on exact boundaries.
    bin_size: float
    bin_merge_factor: int
    min_date: int
    max_date: int
    # False when the source parquet has no TIME column. The index then holds a
    # synthetic day key, so the request side must not filter on dates.
    has_time: bool
    total_rows: int
    # Measured from a real CSV written at build time, not guessed from the schema.
    csv_bytes_per_row: float
    csv_header_bytes: int
    zip_ratio: float
    sample_rows: int
    sample_files: int
    null_position_rows: int
    out_of_range_position_rows: int
    null_time_rows: int
    column_count: int
    # Detects that the source columns changed since the build, which makes
    # csv_bytes_per_row stale. Empty string when it could not be computed.
    schema_fingerprint: str
    last_updated: str

    @property
    def effective_bin_size(self) -> float:
        """Cell width in degrees, for humans. Never use it to compute a bin."""
        return self.bin_size * self.bin_merge_factor

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "uuid": self.uuid,
            "key": self.key,
            "bin_size": self.bin_size,
            "bin_merge_factor": self.bin_merge_factor,
            "min_date": self.min_date,
            "max_date": self.max_date,
            "has_time": self.has_time,
            "total_rows": self.total_rows,
            "csv_bytes_per_row": self.csv_bytes_per_row,
            "csv_header_bytes": self.csv_header_bytes,
            "zip_ratio": self.zip_ratio,
            "sample_rows": self.sample_rows,
            "sample_files": self.sample_files,
            "null_position_rows": self.null_position_rows,
            "out_of_range_position_rows": self.out_of_range_position_rows,
            "null_time_rows": self.null_time_rows,
            "column_count": self.column_count,
            "schema_fingerprint": self.schema_fingerprint,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EstimationSidecarMetadata":
        return cls(
            version=int(data["version"]),
            uuid=str(data["uuid"]),
            key=str(data["key"]),
            bin_size=float(data["bin_size"]),
            bin_merge_factor=int(data.get("bin_merge_factor", 1)),
            min_date=int(data["min_date"]),
            max_date=int(data["max_date"]),
            has_time=bool(data.get("has_time", True)),
            total_rows=int(data.get("total_rows", 0)),
            csv_bytes_per_row=float(data["csv_bytes_per_row"]),
            csv_header_bytes=int(data.get("csv_header_bytes", 0)),
            zip_ratio=float(data["zip_ratio"]),
            sample_rows=int(data.get("sample_rows", 0)),
            sample_files=int(data.get("sample_files", 0)),
            null_position_rows=int(data.get("null_position_rows", 0)),
            out_of_range_position_rows=int(data.get("out_of_range_position_rows", 0)),
            null_time_rows=int(data.get("null_time_rows", 0)),
            column_count=int(data.get("column_count", 0)),
            schema_fingerprint=str(data.get("schema_fingerprint", "")),
            last_updated=str(data.get("last_updated", "")),
        )


def schema_fingerprint(field_names: Optional[Iterable[str]]) -> str:
    """Stable hash of a dataset's column names.

    Both sides compute it from the same source (the API's cached field names),
    so a mismatch means the dataset gained or lost columns since the build and
    ``csv_bytes_per_row`` no longer describes a row of it.

    Returns "" when the names are unknown, which both sides read as
    "cannot compare" rather than "different".
    """
    if not field_names:
        return ""
    joined = ",".join(sorted(str(name) for name in field_names))
    return "sha1:" + hashlib.sha1(joined.encode("utf-8")).hexdigest()
