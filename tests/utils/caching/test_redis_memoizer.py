import logging
import threading
import time
from typing import Any, Generator

import pytest
import redis
from testcontainers.redis import RedisContainer

from data_access_service.models.co_datasource.csiro.csiro_data_src import CsiroS3Access
from data_access_service.utils.caching.memoizer import RedisMemoizer

log = logging.getLogger(__name__)


class TestRedisMemoizer:
    @pytest.fixture(scope="class")
    def redis_container(self) -> Generator[RedisContainer, Any, None]:
        """Start a container matching docker-compose.yml's local dev `redis`
        service (runs the Valkey image for parity with AWS ElastiCache for
        Valkey). Context manager already starts the container; do not call
        start() again."""
        with RedisContainer(image="valkey/valkey:8") as container:
            log.info(
                f"Started Redis-protocol test container on port "
                f"{container.get_exposed_port(container.port)}"
            )
            yield container

    @pytest.fixture
    def client(self, redis_container) -> Generator[redis.Redis, Any, None]:
        client = redis_container.get_client()
        client.flushall()
        yield client
        client.flushall()

    def test_cache_miss_then_hit(self, client):
        memo = RedisMemoizer(namespace="test", ttl_seconds=60, client=client)
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            return {"n": calls}

        assert memo.get_or_compute("k1", factory) == {"n": 1}
        assert memo.get_or_compute("k1", factory) == {"n": 1}
        assert calls == 1

    def test_csiro_access_round_trips_through_pickle(self, client):
        """The CSIRO key is the first real caller, so its dataclass is what has
        to survive the store."""
        memo = RedisMemoizer(namespace="test", ttl_seconds=60, client=client)
        access = CsiroS3Access(
            bucket="csiro-bucket",
            prefix="000072626v004/data/",
            endpoint_url="https://s3.data.csiro.au",
            access_key="key",
            secret_access_key="secret",
        )

        # First call: compute path.
        memo.get_or_compute("access-key", lambda: access)

        # Second call: cache-hit path — exercises the actual pickle round trip.
        def fail_if_called():
            raise AssertionError("factory should not run on a cache hit")

        assert memo.get_or_compute("access-key", fail_if_called) == access

    def test_ttl_expiry_triggers_recompute(self, client):
        memo = RedisMemoizer(namespace="test", ttl_seconds=1, client=client)
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            return calls

        assert memo.get_or_compute("ttl-key", factory) == 1
        time.sleep(1.5)
        assert memo.get_or_compute("ttl-key", factory) == 2

    def test_concurrent_get_or_compute_dedups_across_instances(self, redis_container):
        # Two separate RedisMemoizer instances (own clients) simulate two app
        # instances racing the same cold key.
        client_a = redis_container.get_client()
        client_b = redis_container.get_client()
        client_a.flushall()
        memo_a = RedisMemoizer(namespace="test", ttl_seconds=60, client=client_a)
        memo_b = RedisMemoizer(namespace="test", ttl_seconds=60, client=client_b)

        calls = 0
        calls_lock = threading.Lock()
        barrier = threading.Barrier(2)
        results = {}

        def factory():
            nonlocal calls
            with calls_lock:
                calls += 1
            time.sleep(0.5)
            return "computed-value"

        def call(memo, name):
            barrier.wait()
            results[name] = memo.get_or_compute("race-key", factory)

        t_a = threading.Thread(target=call, args=(memo_a, "a"))
        t_b = threading.Thread(target=call, args=(memo_b, "b"))
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        assert calls == 1
        assert results == {"a": "computed-value", "b": "computed-value"}

    def test_waiter_recomputes_when_the_lock_holder_dies(self, redis_container):
        """A short wait budget must not hang a caller forever: it computes for
        itself once the budget is gone. This is why a slow factory needs a
        bigger budget than the default."""
        client = redis_container.get_client()
        client.flushall()
        memo = RedisMemoizer(
            namespace="test",
            ttl_seconds=60,
            client=client,
            lock_ttl_seconds=30,
            max_wait_seconds=1,
        )
        # A lock with nobody behind it: the holder died before writing a result.
        client.set("test:'orphan':lock", "someone-elses-token", ex=30)

        assert memo.get_or_compute("orphan", lambda: "computed-locally") == (
            "computed-locally"
        )

    def test_fails_open_when_redis_unreachable(self, caplog):
        unreachable_client = redis.Redis(
            host="localhost", port=1, socket_connect_timeout=1, socket_timeout=1
        )
        memo = RedisMemoizer(
            namespace="test", ttl_seconds=60, client=unreachable_client
        )
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            return "fallback-value"

        with caplog.at_level(logging.WARNING):
            assert memo.get_or_compute("unreachable-key", factory) == "fallback-value"
            # Second call should not re-warn (connection errors are de-duplicated).
            assert memo.get_or_compute("unreachable-key", factory) == "fallback-value"

        assert calls == 2
        connection_warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "Cannot connect to Redis/Valkey" in r.getMessage()
        ]
        assert len(connection_warnings) == 1
        msg = connection_warnings[0].getMessage()
        assert "localhost:1" in msg
        assert "cache.backend: none" in msg
        assert "falling back to uncached factory()" in msg
