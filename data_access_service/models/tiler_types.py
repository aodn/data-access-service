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

    Local disk only - no S3, no spill directory needed: each read is a small,
    already-batched point query against a parquet file the batch job wrote
    onto local disk.
    """

    memory_limit: str
    threads: int


@dataclass(frozen=True)
class TilerParquetConfig:
    """Settings for the batch zarr -> parquet conversion job
    (``batch.tiler.generator``), read from the ``tiler_parquet:`` section of
    config.yaml (see Config.get_tiler_parquet_config()).
    """

    # Local output directory - no S3 upload yet, that's follow-up work.
    output_dir: str
    batch_days: int
    # None converts full history; set for a local/dev "latest N" sample.
    max_timestamps: Optional[int]
    # True: the batch run forks one child per store so DuckDB/xarray memory
    # goes back to the OS on exit. False: run in the main process (local debug).
    use_fork_process: bool
