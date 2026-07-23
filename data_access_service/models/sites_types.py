from dataclasses import dataclass


@dataclass(frozen=True)
class ParquetsGenerationConfig:
    """DuckDB tuning for the API's on-disk Parquet client.

    Carries everything
    :class:`~data_access_service.core.duckdbclient.ParquetDuckDBClient` needs to
    build its connection — database path, memory limit, thread count, spill
    (temp) directory, S3 region, and the extensions to load — so the client
    takes no constructor arguments (tests override this config instead).
    """

    duckdb_database: str
    memory_limit: str
    threads: int
    duckdb_temp_dir: str
    region: str
    extensions: tuple[str, ...]
