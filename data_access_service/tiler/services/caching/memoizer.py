"""L1 cache backends: none, or Redis/Valkey (``CACHE_HOST`` when deployed)."""

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
    """A cache that also dedupes across instances."""

    @abstractmethod
    def get_or_compute(self, key: Hashable, factory: Callable[[], T]) -> T:
        """The cached value, else wait for another compute, else run ``factory()``."""


class NullMemoizer(CacheBackend):
    """No cache: every call runs ``factory()``."""

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
    """Shared cache in Redis/Valkey, values pickled.

    On a miss, the first caller takes a short lock and computes; others poll
    for its result, and compute themselves if it takes too long. Any Redis
    error falls back to calling ``factory()`` directly.
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
        # Log a connection failure once, not on every request.
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
        """Log a Redis error; connection errors only once."""
        if isinstance(exc, redis.exceptions.ConnectionError):
            if not self._connection_error_logged:
                detail = str(exc).rstrip(".")
                log.warning(
                    "Cannot connect to Redis/Valkey at %s during %s for key %s: %s. "
                    "%s. "
                    "For local dev without a cache, set CACHE_BACKEND=none "
                    "(or tiler.config.api.cache.backend: none in config.yaml). "
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
    """The backend set by ``tiler.config.api.cache.backend``: "none" or "redis"."""
    cache = Config.get_config().get_tiler_api_config().cache
    backend = cache.backend
    if backend == "none":
        return NullMemoizer()
    if backend == "redis":
        client = redis.Redis(
            host=cache.host,
            port=cache.port,
            socket_connect_timeout=1,
            socket_timeout=1,
            ssl=cache.is_tls,
        )
        return RedisMemoizer(
            namespace=namespace, ttl_seconds=ttl_seconds, client=client
        )
    raise ValueError(f"Unknown CACHE_BACKEND: {backend!r} (expected none or redis)")
