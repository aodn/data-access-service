"""Cross-request dedup + caching for the L1 slice cache, plus backend selection.

``CacheBackend`` is the shared contract; ``NullMemoizer`` and ``RedisMemoizer``
are the current implementations (see ``create_memoizer`` below, chosen via
``CACHE_BACKEND``). ``RedisMemoizer`` talks to a Redis-protocol store — in
deployment that's an AWS ElastiCache for Valkey cluster, addressed via the
``CACHE_HOST`` env var (see ``Config.get_tiler_config``); there's no local
container in docker-compose.yml, so local runs need ``CACHE_HOST`` pointed at
a reachable Redis/Valkey instance or ``CACHE_BACKEND=none`` to skip caching.

In-process dedup-only coalescing (``services.caching.deduper.Deduper``) is a
separate, simpler concern that doesn't fit this module's cache-or-recompute
contract — it never stores anything. Each ``Deduper`` instance lives with its
one consumer (e.g. ``rendering.data_tiles._processed_dedup``,
``store.slice_loader._slice_dedup``), not paired with a ``CacheBackend`` here.

NOT a replacement for ``services.store.registry.StoreRegistry`` — that adds TTL +
stale-while-revalidate + background refresh on top of the dedup pattern, which
this module deliberately does not model.
"""

import logging
import pickle
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
from typing import TypeVar

import redis

from data_access_service.config.config import Config

T = TypeVar("T")

log = logging.getLogger(__name__)


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


_UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class RedisMemoizer(CacheBackend):
    """Distributed cache + cross-instance dedup backed by a Redis-protocol-
    compatible store (Redis or Valkey). Values are pickled — safe here because
    the store is only reachable from this service's own instances, the same
    trust boundary the in-process ``Deduper`` already relies on.

    Cross-instance stampede protection: the first caller for a cold key wins a
    short-lived ``SET NX EX`` lock and runs ``factory()``; other callers poll
    for the winner's result instead of recomputing, falling back to computing
    it themselves if the wait budget is exceeded (bounds latency if the lock
    holder dies mid-compute).

    Any Redis error — connection refused, timeout, etc. — fails open: log a
    warning and call ``factory()`` directly, matching the rest of the tiler's
    bias toward availability over strict caching (``StoreRegistry``'s
    stale-while-revalidate, ``Deduper`` as the load-bearing piece when
    ``CACHE_BACKEND=none``).
    """

    _LOCK_TTL_SECONDS = 30
    _MAX_WAIT_SECONDS = 20
    _POLL_INTERVAL_SECONDS = 0.1

    def __init__(self, *, namespace: str, ttl_seconds: int, client: redis.Redis):
        self._namespace = namespace
        self._ttl_seconds = ttl_seconds
        self._client = client
        self._unlock_script = client.register_script(_UNLOCK_SCRIPT)
        conn_kwargs = client.connection_pool.connection_kwargs
        self._endpoint = (
            f"{conn_kwargs.get('host', 'unknown')}:{conn_kwargs.get('port', 'unknown')}"
        )
        # Connection failures are common in local dev without Redis; log the
        # actionable message once so every request doesn't dump a traceback.
        self._connection_error_logged = False

    def _key(self, key: Hashable) -> str:
        return f"{self._namespace}:{key!r}"

    def _log_redis_error(
        self,
        operation: str,
        redis_key: str,
        exc: BaseException,
        *,
        recovery: str,
    ) -> None:
        """Log Redis failures with host/port context; de-noise connection errors."""
        if isinstance(exc, redis.exceptions.ConnectionError):
            if not self._connection_error_logged:
                # Strip trailing period from redis-py messages so we don't get "refused.."
                detail = str(exc).rstrip(".")
                log.warning(
                    "Cannot connect to Redis/Valkey at %s during %s for key %s: %s. "
                    "%s. "
                    "For local dev without a cache, set CACHE_BACKEND=none "
                    "(or tiler.cache_backend: none in config.yaml). "
                    "To use caching, start Redis/Valkey on that host/port or set "
                    "CACHE_HOST to a reachable instance. Further connection "
                    "failures will be logged at DEBUG.",
                    self._endpoint,
                    operation,
                    redis_key,
                    detail,
                    recovery,
                )
                self._connection_error_logged = True
            else:
                log.debug(
                    "Redis still unreachable at %s during %s for %s; %s",
                    self._endpoint,
                    operation,
                    redis_key,
                    recovery,
                )
            return

        log.warning(
            "Redis %s failed for key %s at %s: %s. %s",
            operation,
            redis_key,
            self._endpoint,
            exc,
            recovery,
            exc_info=True,
        )

    def get_or_compute(self, key: Hashable, factory: Callable[[], T]) -> T:
        redis_key = self._key(key)
        try:
            cached = self._client.get(redis_key)
        except redis.exceptions.RedisError as exc:
            self._log_redis_error(
                "GET",
                redis_key,
                exc,
                recovery="falling back to uncached factory()",
            )
            return factory()

        if cached is not None:
            return pickle.loads(cached)

        lock_key = f"{redis_key}:lock"
        token = uuid.uuid4().hex
        try:
            acquired = self._client.set(
                lock_key, token, nx=True, ex=self._LOCK_TTL_SECONDS
            )
        except redis.exceptions.RedisError as exc:
            self._log_redis_error(
                "lock acquire",
                redis_key,
                exc,
                recovery="falling back to uncached factory()",
            )
            return factory()

        if acquired:
            try:
                result = factory()
                try:
                    self._client.set(
                        redis_key, pickle.dumps(result), ex=self._ttl_seconds
                    )
                except redis.exceptions.RedisError as exc:
                    self._log_redis_error(
                        "SET",
                        redis_key,
                        exc,
                        recovery="result computed but not cached",
                    )
            finally:
                try:
                    self._unlock_script(keys=[lock_key], args=[token])
                except redis.exceptions.RedisError as exc:
                    self._log_redis_error(
                        "unlock",
                        lock_key,
                        exc,
                        recovery="lock will expire via TTL",
                    )
            return result

        return self._wait_for_result(redis_key, factory)

    def _wait_for_result(self, redis_key: str, factory: Callable[[], T]) -> T:
        deadline = time.monotonic() + self._MAX_WAIT_SECONDS
        while time.monotonic() < deadline:
            time.sleep(self._POLL_INTERVAL_SECONDS)
            try:
                cached = self._client.get(redis_key)
            except redis.exceptions.RedisError as exc:
                self._log_redis_error(
                    "poll",
                    redis_key,
                    exc,
                    recovery="falling back to uncached factory()",
                )
                return factory()
            if cached is not None:
                return pickle.loads(cached)
        log.warning(
            "Timed out waiting for in-flight compute of %s; recomputing locally",
            redis_key,
        )
        return factory()


def create_memoizer(*, namespace: str, ttl_seconds: int) -> CacheBackend:
    """Selects the L1 cache backend via the CACHE_BACKEND setting.

    - "none" (default): bypass caching entirely — every call recomputes.
    - "redis": share cache + cross-instance dedup through a Redis-protocol
      store, connected via ``tiler.redis_host``/``tiler.redis_port`` in
      config.yaml.
    """
    tiler_config = Config.get_config().get_tiler_config()
    backend = tiler_config.cache_backend
    if backend == "none":
        return NullMemoizer()
    if backend == "redis":
        client = redis.Redis(host=tiler_config.redis_host, port=tiler_config.redis_port)
        return RedisMemoizer(
            namespace=namespace, ttl_seconds=ttl_seconds, client=client
        )
    raise ValueError(f"Unknown CACHE_BACKEND: {backend!r} (expected none or redis)")
