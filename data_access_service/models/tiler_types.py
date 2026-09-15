from dataclasses import dataclass


@dataclass(frozen=True)
class TilerVectorConfig:
    """DuckDB tuning + output layout for time-sliced vector cells.

    Read from the ``tiler.vector`` block of config.yaml (see
    ``Config.get_tiler_vector_config``). The request-path client uses
    ``memory_limit`` / ``threads``; batch writes parquet under ``output_dir``.
    """

    duckdb_database: str
    duckdb_temp_dir: str
    memory_limit: str
    threads: int
    region: str
    output_dir: str
    max_cells_long_edge: int
    s3_prefix: str
    s3_bucket: str
    write_s3: bool
    keep_local_parquet: bool
    row_group_size: int
    # 0 = no cap (all time steps). Dev/test set a small n to verify faster.
    max_time_slices: int


@dataclass(frozen=True)
class TilerConfig:
    """Runtime tuning for the tiler app (data_access_service/tiler), read from
    the ``tiler:`` section of config.yaml (see Config.get_tiler_config()).
    """

    co_bucket: str
    store_prewarm_workers: int
    store_refresh_interval_hours: int
    thread_pool_size: int
    animation_workers: int
    cache_backend: str
    slice_cache_ttl_seconds: int
    redis_host: str
    redis_port: int
    is_tls: bool
    s3_anon: bool
    s3_connect_timeout: int
    s3_read_timeout: int
    s3_max_attempts: int
