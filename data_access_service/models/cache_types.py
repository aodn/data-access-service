from dataclasses import dataclass


@dataclass(frozen=True)
class CacheConfig:
    # "none" (no caching at all) or "redis".
    backend: str
    host: str
    port: int
    # Only the deployed endpoint speaks TLS, so this follows CACHE_HOST rather
    # than being set in yaml.
    is_tls: bool
