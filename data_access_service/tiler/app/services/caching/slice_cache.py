"""L2 cache for fully-computed (store, date, variables) slices.

Exposes the CacheBackend so slice loading can call ``get_or_compute``
directly. In-process dedup (independent of ``CACHE_BACKEND``) is a separate
concern that lives with its one consumer — see ``store/slice_loader.py``.
"""

from data_access_service.tiler.app.config import settings
from data_access_service.tiler.app.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)


# Backend is selectable via CACHE_BACKEND (see memoizer.create_memoizer) so
# multiple instances can share L2 through a distributed backend instead of each
# holding a private copy.
slice_memo: CacheBackend = create_memoizer(
    namespace="l2", ttl_seconds=settings.SLICE_CACHE_TTL_SECONDS
)
