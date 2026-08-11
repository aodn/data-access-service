import dataclasses

import pytest

from data_access_service.config.config import Config
from data_access_service.tiler.services.caching.memoizer import (
    CacheBackend,
    NullMemoizer,
    RedisMemoizer,
    create_memoizer,
)


def _patch_cache_backend(monkeypatch, backend: str):
    config = Config.get_config()
    original = config.get_tiler_config()
    monkeypatch.setattr(
        config,
        "get_tiler_config",
        lambda: dataclasses.replace(original, cache_backend=backend),
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
    # config.yaml's tiler.cache_backend default is "redis"; this checks
    # create_memoizer honors it without needing _patch_cache_backend.
    memo = create_memoizer(namespace="l1", ttl_seconds=60)
    assert isinstance(memo, RedisMemoizer)


def test_none_backend(monkeypatch):
    _patch_cache_backend(monkeypatch, "none")
    memo = create_memoizer(namespace="l1", ttl_seconds=60)
    assert isinstance(memo, NullMemoizer)


def test_redis_backend(monkeypatch):
    # redis.Redis(...) is lazy — it doesn't connect until the first command,
    # so this doesn't need a live server.
    _patch_cache_backend(monkeypatch, "redis")
    memo = create_memoizer(namespace="l1", ttl_seconds=60)
    assert isinstance(memo, RedisMemoizer)


def test_unknown_backend_raises(monkeypatch):
    _patch_cache_backend(monkeypatch, "disk")
    with pytest.raises(ValueError, match="disk"):
        create_memoizer(namespace="l1", ttl_seconds=60)
