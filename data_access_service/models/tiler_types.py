from dataclasses import dataclass


@dataclass(frozen=True)
class TilerConfig:
    """Runtime tuning for the tiler app (data_access_service/tiler), read from
    the ``tiler:`` section of config.yaml (see Config.get_tiler_config()).
    """

    tile_timezone: str
    store_ttl_seconds: int
    store_prewarm_workers: int
    thread_pool_size: int
    animation_workers: int
    cache_backend: str
    slice_cache_ttl_seconds: int
    processed_cache_ttl_seconds: int
    s3_anon: bool
    s3_connect_timeout: int
    s3_read_timeout: int
    s3_max_attempts: int
