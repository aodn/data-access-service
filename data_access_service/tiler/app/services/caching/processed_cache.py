"""L1 cache for processed (resampled + normalised) grids.

The processed grid for one (product, date, LOD) is shared across every tile in
that LOD — N×M tiles draw from the same numpy arrays. This cache holds those
arrays so the resample + normalize cost is paid once per LOD instead of per
tile.

Exposes the CacheBackend so the rendering pipeline can call ``get_or_compute``
directly. In-process dedup (independent of ``CACHE_BACKEND``) is a separate
concern that lives with its one consumer — see ``rendering/data_tiles.py``.
"""

from data_access_service.config.config import Config
from data_access_service.tiler.app.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)


# Backend is selectable via CACHE_BACKEND (see memoizer.create_memoizer) so
# multiple instances can share L1 through a distributed backend instead of each
# holding a private copy.
processed_memo: CacheBackend = create_memoizer(
    namespace="l1",
    ttl_seconds=Config.get_config().get_tiler_config().processed_cache_ttl_seconds,
)
