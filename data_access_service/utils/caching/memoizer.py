"""One cached value per key, shared by every container of this service.

``cache.backend`` in config.yaml picks ``NullMemoizer`` or ``RedisMemoizer``
(Valkey in AWS, the docker-compose ``redis`` service locally).

Any backend may compute the value itself, so treat caching as an optimisation.
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
    backend for ``cache.backend: none``; a stampede of concurrent identical
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
    the store sits inside the VPC and only this service's own containers can
    reach it. Nothing a viewer supplies is ever cached.

    Cross-instance stampede protection: the first caller for a cold key wins a
    short-lived ``SET NX EX`` lock and runs ``factory()``; other callers poll
    for the winner's result instead of recomputing, falling back to computing
    it themselves if the wait budget is exceeded (bounds latency if the lock
    holder dies mid-compute).

    Any Redis error — connection refused, timeout, etc. — fails open: log a
    warning and call ``factory()`` directly. Availability beats strict caching:
    a caller must still work with the cache switched off.

    ``lock_ttl_seconds`` and ``max_wait_seconds`` must both leave room for the
    slowest ``factory()`` this instance is given. If the lock expires while its
    holder is still working, every waiter computes too — the stampede this
    class exists to prevent. The defaults suit a factory of a few seconds.
    """

    DEFAULT_LOCK_TTL_SECONDS = 30
    DEFAULT_MAX_WAIT_SECONDS = 20

    _POLL_INTERVAL_SECONDS = 0.1

    def __init__(
        self,
        *,
        namespace: str,
        ttl_seconds: int,
        client: redis.Redis,
        lock_ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
        max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    ):
        self._namespace = namespace
        self._ttl_seconds = ttl_seconds
        self._lock_ttl_seconds = lock_ttl_seconds
        self._max_wait_seconds = max_wait_seconds
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
                    "For local dev without a cache, set cache.backend: none "
                    "in config.yaml. "
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
            log.info("Redis cache hit for key %s at %s", redis_key, self._endpoint)
            return pickle.loads(cached)

        lock_key = f"{redis_key}:lock"
        token = uuid.uuid4().hex
        try:
            acquired = self._client.set(
                lock_key, token, nx=True, ex=self._lock_ttl_seconds
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
                    log.info(
                        "Redis cache write for key %s at %s (ttl=%ss)",
                        redis_key,
                        self._endpoint,
                        self._ttl_seconds,
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
        deadline = time.monotonic() + self._max_wait_seconds
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
                log.info(
                    "Redis cache hit for key %s at %s (after waiting on in-flight compute)",
                    redis_key,
                    self._endpoint,
                )
                return pickle.loads(cached)
        log.warning(
            "Timed out waiting for in-flight compute of %s; recomputing locally",
            redis_key,
        )
        return factory()


def create_memoizer(
    *,
    namespace: str,
    ttl_seconds: int,
    lock_ttl_seconds: int = RedisMemoizer.DEFAULT_LOCK_TTL_SECONDS,
    max_wait_seconds: int = RedisMemoizer.DEFAULT_MAX_WAIT_SECONDS,
) -> CacheBackend:
    """Builds the cache backend named by ``cache.backend`` in config.yaml.

    - "none": bypass caching entirely — every call recomputes.
    - "redis": share the value, and the work of computing it, with every other
      container through the store at ``cache.host``/``cache.port`` (or
      ``CACHE_HOST``).

    ``namespace`` keeps one caller's keys apart from another's. Callers with a
    slow factory pass their own lock/wait budget — see ``RedisMemoizer``.
    """
    cache_config = Config.get_config().get_cache_config()
    backend = cache_config.backend
    if backend == "none":
        return NullMemoizer()
    if backend == "redis":
        # Short socket timeouts on purpose: a slow cache must never hold up the
        # caller, which can always compute the value itself.
        client = redis.Redis(
            host=cache_config.host,
            port=cache_config.port,
            socket_connect_timeout=1,
            socket_timeout=1,
            ssl=cache_config.is_tls,
        )
        return RedisMemoizer(
            namespace=namespace,
            ttl_seconds=ttl_seconds,
            client=client,
            lock_ttl_seconds=lock_ttl_seconds,
            max_wait_seconds=max_wait_seconds,
        )
    raise ValueError(f"Unknown cache.backend: {backend!r} (expected none or redis)")
