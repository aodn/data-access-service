"""The L1 slice cache, keyed by (store, date, variables)."""

from data_access_service.config.config import Config
from data_access_service.tiler.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)

slice_memo: CacheBackend = create_memoizer(
    namespace="l1",
    ttl_seconds=Config.get_config().get_tiler_api_config().cache.ttl_seconds,
)
