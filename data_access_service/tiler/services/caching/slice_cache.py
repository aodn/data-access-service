"""L1 cache for fully-computed (store, date, variables) slices.

Exposes the CacheBackend so slice loading can call ``get_or_compute``
directly. In-process dedup (independent of ``CACHE_BACKEND``) is a separate
concern that lives with its one consumer — see ``store/slice_loader.py``.

Default is ``none``: origin does not retain slices; CloudFront caches tile
bytes. Concurrent identical ``load_slice`` calls still share one compute via
``_slice_dedup``.
"""

from data_access_service.config.config import Config
from data_access_service.tiler.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)

# Backend is selectable via tiler.cache_backend (see memoizer.create_memoizer).
slice_memo: CacheBackend = create_memoizer(
    namespace="l1",
    ttl_seconds=Config.get_config().get_tiler_config().slice_cache_ttl_seconds,
)
