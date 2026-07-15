"""Cross-request dedup + caching for the L1/L2 cache stack, plus backend selection.

``CacheBackend`` is the shared contract; ``NullMemoizer`` is currently the
only implementation (see ``create_memoizer`` below, chosen via
``CACHE_BACKEND``). A distributed backend (e.g. Redis-backed, for sharing
cache state across ECS instances) was removed for now — to bring one back,
add a class implementing ``CacheBackend`` and wire it into ``create_memoizer``.

In-process dedup-only coalescing (``services.caching.deduper.Deduper``) is a
separate, simpler concern that doesn't fit this module's cache-or-recompute
contract — it never stores anything. Each ``Deduper`` instance lives with its
one consumer (e.g. ``rendering.data_tiles._processed_dedup``,
``store.slice_loader._slice_dedup``), not paired with a ``CacheBackend`` here.

NOT a replacement for ``services.store.registry.StoreRegistry`` — that adds TTL +
stale-while-revalidate + background refresh on top of the dedup pattern, which
this module deliberately does not model.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
from typing import TypeVar

from data_access_service.config.config import Config

T = TypeVar("T")


class CacheBackend(ABC):
    """Shared contract for cache + cross-instance dedup implementations.

    ``get_or_compute`` is the only method any production caller invokes.
    """

    @abstractmethod
    def get_or_compute(self, key: Hashable, factory: Callable[[], T]) -> T:
        """Return cached value, wait on an in-flight compute, or run ``factory()`` once."""


class NullMemoizer(CacheBackend):
    """No caching, no dedup — every call runs ``factory()``. Explicit opt-out
    backend for ``CACHE_BACKEND=none``; a stampede of concurrent identical
    requests will all recompute, which is the accepted cost of disabling
    caching entirely."""

    def get_or_compute(self, key: Hashable, factory: Callable[[], T]) -> T:
        return factory()


# TODO: add a RedisMemoizer (or other distributed backend) implementing CacheBackend


def create_memoizer(*, namespace: str, ttl_seconds: int) -> CacheBackend:
    """Selects the L1/L2 cache backend via the CACHE_BACKEND setting.

    - "none" (default): bypass caching entirely — every call recomputes.
    - Any other backend (e.g. a shared cache for deployments running more than
      one instance) needs a class implementing ``CacheBackend`` wired in here;
      none is currently implemented.
    """
    backend = Config.get_config().get_tiler_config().cache_backend
    if backend == "none":
        return NullMemoizer()
    # if backend == "redis":
    #     return RedisMemoizer(namespace=namespace, ttl_seconds=ttl_seconds)
    raise ValueError(f"Unknown CACHE_BACKEND: {backend!r} (expected none)")
