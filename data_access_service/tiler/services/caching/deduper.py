"""Share one in-flight computation between concurrent callers."""

import concurrent.futures
import threading
from collections.abc import Callable, Hashable
from typing import TypeVar

T = TypeVar("T")


class Deduper:
    """If a call for ``key`` is already running, wait for its result instead
    of running ``factory()`` again. Nothing is cached.

    Blocks, so call it from a worker thread, not the event loop. Also saves
    Redis round trips: only one thread per process asks Redis for a key.
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
