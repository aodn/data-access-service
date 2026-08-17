"""In-process in-flight request coalescing — no caching, nothing to evict."""

import concurrent.futures
import threading
from collections.abc import Callable, Hashable
from typing import TypeVar

T = TypeVar("T")


class Deduper:
    """Stops concurrent callers with the same key from redoing the same work.

    Content-agnostic: doesn't know or care what ``factory()`` does (S3 fetch,
    resample, render, ...). If a call for ``key`` is already in flight, later
    callers block and share its result instead of each running ``factory()``
    themselves. Must be driven from a worker thread (sync ``def`` handler or
    ``anyio.to_thread.run_sync``), never directly from an ``async def`` on the
    event loop — waiting on the result is a blocking call.

    Still worth it even where a distributed-lock ``CacheBackend`` (e.g. a future
    Redis-backed one, ``CACHE_BACKEND=redis``) sits behind it:

    - A distributed lock only dedupes when a non-"none" backend is actually
      configured. ``CACHE_BACKEND=none`` (this project's default) has no lock
      at all, so this is the *only* protection against a concurrent burst
      redoing the same work — not an optimisation on top of something else.
    - Even with the distributed lock enabled, without this every thread in a
      local burst would each make its own round trip to the backend (e.g. a
      failed `SET NX` + a `pubsub` subscribe + wait) to discover someone else
      already won. This coalesces the whole local burst into one contender
      first, so only one thread per process ever touches the backend for a
      given key.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight: dict[Hashable, concurrent.futures.Future] = {}

    def dedupe(self, key: Hashable, factory: Callable[[], T]) -> T:
        should_compute = False
        with self._lock:
            if key in self._inflight:
                future = self._inflight[key]
            else:
                future = concurrent.futures.Future()
                self._inflight[key] = future
                should_compute = True

        if not should_compute:
            return future.result()  # type: ignore[no-any-return]

        try:
            result = factory()
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            with self._lock:
                self._inflight.pop(key, None)
        return result
