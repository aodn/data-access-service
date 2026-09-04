from dataclasses import dataclass


@dataclass(frozen=True)
class SitesConfig:
    """DuckDB tuning for the sites API's on-disk Parquet client.

    Carries everything
    :class:`~data_access_service.core.duckdbclient.SitesDuckDBClient` needs to
    build its connection — database path, memory limit, thread count, spill
    (temp) directory, S3 region, and the extensions to load — so the client
    takes no constructor arguments (tests override this config instead).
    """

    duckdb_database: str
    co_bucket: str
    memory_limit: str
    threads: int
    duckdb_temp_dir: str
    region: str
    extensions: tuple[str, ...]
