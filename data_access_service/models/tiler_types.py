from dataclasses import dataclass
from typing import Optional


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


@dataclass(frozen=True)
class TilerDuckDBConfig:
    """DuckDB settings for reading batch-generated parquet slices inside the
    live tiler API (``TilerDuckDBClient``), read from the ``tiler_duckdb:``
    section of config.yaml (see Config.get_tiler_duckdb_config()).

    Reads are small, already-batched point queries against parquet files the
    batch job wrote to S3 - no spill directory needed.
    """

    memory_limit: str
    threads: int


@dataclass(frozen=True)
class TilerParquetConfig:
    """Settings for the batch zarr -> parquet conversion job
    (``batch.tiler.generator``), read from the ``tiler_parquet:`` section of
    config.yaml (see Config.get_tiler_parquet_config()).

    ``output_dir`` is the base every parquet/metadata path is written under
    and read back from - always ``s3://{datavis_data bucket}/{s3_prefix}``
    (see Config.get_tiler_parquet_config()). Batch and the live tiler always
    agree on it, since both read the same config.
    """

    output_dir: str
    batch_days: int
    # None converts full history; set for a "latest N" sample run.
    max_timestamps: Optional[int]
    # True: the batch run forks one child per store so DuckDB/xarray memory
    # goes back to the OS on exit. False: run in the main process (debug).
    use_fork_process: bool
