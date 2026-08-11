import logging
import threading
import time
from typing import Any, Generator

import numpy as np
import pytest
import redis
import xarray as xr
from testcontainers.redis import RedisContainer

from data_access_service.tiler.services.caching.memoizer import RedisMemoizer

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

    def test_xarray_dataset_round_trips_through_pickle(self, client):
        memo = RedisMemoizer(namespace="test", ttl_seconds=60, client=client)
        ds = xr.Dataset(
            {"sst": (("lat", "lon"), np.array([[1.0, 2.0], [3.0, 4.0]]))},
            coords={"lat": [10.0, 20.0], "lon": [100.0, 110.0]},
        )

        # First call: compute path.
        memo.get_or_compute("ds-key", lambda: ds)

        # Second call: cache-hit path — exercises the actual pickle round trip.
        def fail_if_called():
            raise AssertionError("factory should not run on a cache hit")

        cached_result = memo.get_or_compute("ds-key", fail_if_called)
        xr.testing.assert_identical(cached_result, ds)

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

    def test_fails_open_when_redis_unreachable(self):
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

        assert memo.get_or_compute("unreachable-key", factory) == "fallback-value"
        assert calls == 1
