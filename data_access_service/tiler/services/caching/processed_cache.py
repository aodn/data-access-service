"""L1 cache for processed grids, shared by the data-tile and visual-tile pipelines.

  * ``data_processed_memo`` — data_tiles' resampled + normalised LOD grid, shared
    across every tile in that LOD (see ``rendering/data_tiles.py``).
  * ``visual_processed_memo`` — visual_tiles' inpaint + land-cut filled array,
    shared across every tile/bbox/animation-frame for a (product, date) (see
    ``rendering/visual_tiles.py``). This only enabled for product whose coastal_fill is not none.

Both pay their processing cost once instead of once per rendered tile.

Exposes the CacheBackend so each rendering pipeline can call ``get_or_compute``
directly. In-process dedup (independent of ``CACHE_BACKEND``) is a separate
concern that lives with each pipeline's one consumer — see
``rendering/data_tiles.py`` (``_processed_dedup``) and
``rendering/visual_tiles.py`` (``_fill_dedup``).
"""

from data_access_service.config.config import Config
from data_access_service.tiler.services.caching.memoizer import (
    CacheBackend,
    create_memoizer,
)


# Backend is selectable via CACHE_BACKEND (see memoizer.create_memoizer) so
# multiple instances can share L1 through a distributed backend instead of each
# holding a private copy. Separate namespaces so a future distributed backend
# never key-collides the two pipelines' caches.
data_processed_memo: CacheBackend = create_memoizer(
    namespace="l1_data",
    ttl_seconds=Config.get_config().get_tiler_config().processed_cache_ttl_seconds,
)

visual_processed_memo: CacheBackend = create_memoizer(
    namespace="l1_visual",
    ttl_seconds=Config.get_config().get_tiler_config().processed_cache_ttl_seconds,
)
