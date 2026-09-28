"""The CSIRO temporary key is fetched once and shared by every container.

Every container used to ask CSIRO for its own key, which is how a load test made
CSIRO answer 417 (backlog#9304) and how one slow reply stopped a container from
starting (backlog#9137).
"""

import logging
import threading
import time
from typing import Any, Generator

import pytest
import redis
from testcontainers.redis import RedisContainer

from data_access_service.config.config import Config
from data_access_service.models.co_datasource.co_data_registory import (
    resolve_dataset_location,
)
from data_access_service.models.co_datasource.csiro import csiro_data_src
from data_access_service.utils.caching.memoizer import NullMemoizer, RedisMemoizer

from tests.models.co_datasource.test_dataset_location import (
    CSIRO_DATASET,
    _patch_keys,
)

log = logging.getLogger(__name__)

# Long enough that nothing expires mid-test, short enough to stay a test value.
TTL_SECONDS = 60


class TestCsiroKeyCache:
    @pytest.fixture(scope="class")
    def redis_container(self) -> Generator[RedisContainer, Any, None]:
        with RedisContainer(image="valkey/valkey:8") as container:
            yield container

    @pytest.fixture
    def shared_cache(self, redis_container, monkeypatch) -> redis.Redis:
        """Point the key lookup at a real store, as a deployed container has."""
        client = redis_container.get_client()
        client.flushall()
        monkeypatch.setattr(
            csiro_data_src,
            "_key_memo",
            RedisMemoizer(
                namespace=csiro_data_src._CSIRO_KEY_CACHE_NAMESPACE,
                ttl_seconds=TTL_SECONDS,
                client=client,
            ),
        )
        return client

    def test_second_container_reads_the_cached_key(self, shared_cache):
        with _patch_keys() as first:
            first_location = resolve_dataset_location(CSIRO_DATASET)

        # No side_effect responses left: if this called CSIRO it would raise.
        with _patch_keys() as second:
            second_location = resolve_dataset_location(CSIRO_DATASET)

        assert first.call_count == 2, "the first caller makes both CSIRO calls"
        assert second.call_count == 0, "the second caller must not ask CSIRO"
        assert second_location == first_location
        assert second_location.access_key == "csiro-key"

    def test_key_is_cached_under_dataset_and_pid(self, shared_cache):
        with _patch_keys():
            resolve_dataset_location(CSIRO_DATASET)

        keys = [k.decode() for k in shared_cache.keys("csiro-key:*")]
        assert len(keys) == 1
        assert CSIRO_DATASET in keys[0]
        assert "csiro:72626" in keys[0]

    def test_expired_entry_is_fetched_again(self, redis_container, monkeypatch):
        """The cache must not outlive the key it holds."""
        client = redis_container.get_client()
        client.flushall()
        monkeypatch.setattr(
            csiro_data_src,
            "_key_memo",
            RedisMemoizer(
                namespace=csiro_data_src._CSIRO_KEY_CACHE_NAMESPACE,
                ttl_seconds=1,
                client=client,
            ),
        )

        with _patch_keys() as first:
            resolve_dataset_location(CSIRO_DATASET)
        time.sleep(1.5)
        with _patch_keys() as after_expiry:
            resolve_dataset_location(CSIRO_DATASET)

        assert first.call_count == 2
        assert after_expiry.call_count == 2

    def test_two_containers_starting_together_ask_csiro_once(self, redis_container):
        """The case that broke: many containers starting at the same moment."""
        client_a = redis_container.get_client()
        client_b = redis_container.get_client()
        client_a.flushall()

        def memoizer(client):
            return RedisMemoizer(
                namespace=csiro_data_src._CSIRO_KEY_CACHE_NAMESPACE,
                ttl_seconds=TTL_SECONDS,
                client=client,
            )

        calls = 0
        calls_lock = threading.Lock()
        barrier = threading.Barrier(2)
        results = {}

        def slow_csiro_reply(dataset_name, fedora_pid):
            nonlocal calls
            with calls_lock:
                calls += 1
            time.sleep(0.5)
            return csiro_data_src.CsiroS3Access(
                bucket="dapprd-mnf",
                prefix="000072626v004/data/",
                endpoint_url="https://s3.data.csiro.au",
                access_key="csiro-key",
                secret_access_key="csiro-secret",
            )

        def start_container(memo, name):
            barrier.wait()
            results[name] = memo.get_or_compute(
                (CSIRO_DATASET, "csiro:72626"),
                lambda: slow_csiro_reply(CSIRO_DATASET, "csiro:72626"),
            )

        threads = [
            threading.Thread(target=start_container, args=(memoizer(client_a), "a")),
            threading.Thread(target=start_container, args=(memoizer(client_b), "b")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert calls == 1, "only one container may ask CSIRO"
        assert results["a"] == results["b"]

    def test_still_works_with_caching_switched_off(self, monkeypatch):
        """``cache.backend: none``, or an unreachable cache, must not break a
        read - it only costs an extra CSIRO call."""
        monkeypatch.setattr(csiro_data_src, "_key_memo", NullMemoizer())

        with _patch_keys():
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.access_key == "csiro-key"

    def test_lock_budget_covers_the_slowest_key_request(self):
        """A lock that expires while CSIRO is still answering lets every waiter
        call CSIRO too, which is the stampede the cache exists to stop."""
        csiro = Config.get_config().get_csiro_config()
        budget = csiro_data_src._KEY_REQUEST_MAX_SECONDS

        # Two calls, each tried 3 times, each waiting up to request_timeout_seconds.
        assert budget >= csiro.request_timeout_seconds * 3 * 2
        assert budget > RedisMemoizer.DEFAULT_LOCK_TTL_SECONDS
