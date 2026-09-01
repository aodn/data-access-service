"""Per-job DuckDB session settings.

Batch jobs share :class:`~data_access_service.core.duckdbclient.PmTileDuckDBClient`
but not its tuning: pmtiles has to leave room for the tippecanoe subprocess,
the estimation index does not. Each job builds its own instance of this and
passes it to the client, so changing one job's limits cannot move the other's.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DuckDBTuningConfig:
    """What PmTileDuckDBClient needs to open a connection."""

    # Source bucket the s3 secret is created for.
    co_bucket: str
    # Prefix of the TemporaryDirectory holding the database and any spill.
    duckdb_temp_dir: str
    # ":memory:" or a file name created inside the temp dir above.
    duckdb_database: str
    memory_limit: str
    threads: int
    show_progress: bool
    enable_external_file_cache: bool = True
