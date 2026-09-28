import dataclasses

import pytest

from data_access_service.config.config import Config
from data_access_service.utils.caching.memoizer import (
    CacheBackend,
    NullMemoizer,
    RedisMemoizer,
    create_memoizer,
)


def _patch_backend(monkeypatch, backend: str):
    config = Config.get_config()
    original = config.get_cache_config()
    monkeypatch.setattr(
        config,
        "get_cache_config",
        lambda: dataclasses.replace(original, backend=backend),
    )


def test_null_memoizer_always_recomputes():
    m = NullMemoizer()
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return calls

    assert m.get_or_compute("k", factory) == 1
    assert m.get_or_compute("k", factory) == 2


def test_null_memoizer_is_a_cache_backend():
    assert isinstance(NullMemoizer(), CacheBackend)


def test_uses_configured_backend_without_patching():
    # config.yaml's cache.backend default is "redis"; this checks
    # create_memoizer honors it without needing _patch_backend.
    memo = create_memoizer(namespace="test", ttl_seconds=60)
    assert isinstance(memo, RedisMemoizer)


def test_none_backend(monkeypatch):
    _patch_backend(monkeypatch, "none")
    assert isinstance(create_memoizer(namespace="test", ttl_seconds=60), NullMemoizer)


def test_redis_backend(monkeypatch):
    # redis.Redis(...) is lazy — it doesn't connect until the first command,
    # so this doesn't need a live server.
    _patch_backend(monkeypatch, "redis")
    assert isinstance(create_memoizer(namespace="test", ttl_seconds=60), RedisMemoizer)


def test_unknown_backend_raises(monkeypatch):
    _patch_backend(monkeypatch, "disk")
    with pytest.raises(ValueError, match="disk"):
        create_memoizer(namespace="test", ttl_seconds=60)


def test_lock_budget_defaults_and_overrides(monkeypatch):
    """A caller with a slow factory must be able to raise the lock/wait budget,
    or its lock expires mid-compute and every waiter recomputes."""
    _patch_backend(monkeypatch, "redis")

    default = create_memoizer(namespace="test", ttl_seconds=60)
    assert default._lock_ttl_seconds == RedisMemoizer.DEFAULT_LOCK_TTL_SECONDS
    assert default._max_wait_seconds == RedisMemoizer.DEFAULT_MAX_WAIT_SECONDS

    slow = create_memoizer(
        namespace="test",
        ttl_seconds=60,
        lock_ttl_seconds=220,
        max_wait_seconds=220,
    )
    assert (slow._lock_ttl_seconds, slow._max_wait_seconds) == (220, 220)
