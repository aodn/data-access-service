"""The L1 slice cache, keyed by (store, date, variables)."""

from data_access_service.config.config import Config
from data_access_service.tiler.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)
from data_access_service.tiler.services.caching.slice_codec import decode, encode

_NAMESPACE = "l1_v1_slice_cache"

slice_memo: CacheBackend = create_memoizer(
    namespace=_NAMESPACE,
    ttl_seconds=Config.get_config().get_tiler_api_config().cache.ttl_seconds,
    dumps=encode,
    loads=decode,
)
