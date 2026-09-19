from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TilerConfig:
    """Runtime tuning for the tiler app (data_access_service/tiler), read from
    the ``tiler:`` section of config.yaml (see Config.get_tiler_config()).
    """

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
    """DuckDB settings for the live tiler's read-side :class:`TilerDuckDBClient`
    (``tiler_duckdb:`` section, see Config.get_tiler_duckdb_config()): small
    point queries against the batch-written parquet, so no spill directory.
    """

    memory_limit: str
    threads: int


@dataclass(frozen=True)
class TilerBatchDuckDBConfig:
    """DuckDB settings for the batch conversion's :class:`TilerBatchDuckDBClient`
    (``tiler_parquet.config.duckdb:``, see Config.get_tiler_parquet_config()),
    one per forked store: a much bigger budget, plus a spill directory.
    """

    memory_limit: str
    threads: int
    # Prefix of the spill directory each client creates and removes itself.
    temp_dir_prefix: str


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
    # Days back from each store's latest timestamp to convert; None converts
    # the full history.
    window_days: Optional[int]
    # Tuning for the batch job's own TilerBatchDuckDBClient (one per
    # store), read from ``tiler_parquet.config.duckdb:``.
    duckdb: TilerBatchDuckDBConfig
    # Fork one worker per store. Off for local macOS runs, where a child
    # forked after the parent touched the network stack segfaults.
    use_fork_process: bool = True
