from dataclasses import dataclass


@dataclass(frozen=True)
class TilerDuckDBConfig:
    """The live tiler's read-side :class:`TilerDuckDBClient`
    (``tiler.config.api.duckdb``)."""

    memory_limit: str
    threads: int


@dataclass(frozen=True)
class TilerApiConfig:
    """The live tiler API (``tiler.config.api``)."""

    store_refresh_interval_hours: int
    thread_pool_size: int
    animation_workers: int
    # RSS above which a tile trims glibc's arenas before it starts; set it
    # out of reach to turn trimming off.
    trim_threshold_mb: int
    duckdb: TilerDuckDBConfig


@dataclass(frozen=True)
class TilerBatchDuckDBConfig:
    """The batch conversion's :class:`TilerBatchDuckDBClient`
    (``tiler.config.batch.duckdb``)."""

    memory_limit: str
    threads: int
    # Prefix of the spill directory each client creates and removes itself.
    temp_dir_prefix: str


@dataclass(frozen=True)
class TilerBatchConfig:
    """The batch zarr -> parquet job (``tiler.config.batch``)."""

    # Where batch writes; the same prefix the API reads from.
    tiler_root_dir: str
    # Cap on zarr time chunks read per store per run, for quick dev runs;
    # None converts everything missing.
    max_chunks_per_run: int | None
    # Fork one worker per store. Off for local macOS runs, where a child
    # forked after the parent touched the network stack segfaults.
    use_fork_process: bool
    duckdb: TilerBatchDuckDBConfig
