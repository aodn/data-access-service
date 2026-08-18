"""
Run one job per key, and let callers that arrive while it runs share its result.

The portal can fire the same estimate several times (retry, re-render, two tabs).
Without this, each one starts its own full estimate of exactly the same thing.
"""

import logging
import threading
from typing import Any, Callable, Optional

from data_access_service.utils.cancellation import (
    Cancellation,
    ClientGoneError,
    GroupCancellation,
    raise_if_client_gone,
)

log = logging.getLogger(__name__)

# How often a waiting caller wakes up to check whether its own client has gone.
FOLLOWER_POLL_INTERVAL: float = 0.5


class _Job:
    def __init__(self) -> None:
        self.group = GroupCancellation()
        self.done = threading.Event()
        self.result: Any = None
        self.error: Optional[BaseException] = None


class SingleFlight:
    """
    One in-flight job per key.

    The first caller for a key ("leader") runs the work; callers that arrive
    while it runs ("followers") block until it finishes and get the same result.
    """

    def __init__(self, poll_interval: float = FOLLOWER_POLL_INTERVAL) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}
        self._poll_interval = poll_interval

    def run(
        self,
        key: str,
        fn: Callable[[GroupCancellation], Any],
        cancellation: Optional[Cancellation] = None,
    ) -> Any:
        """
        Run fn, or attach to the job already running under this key.

        :param key: identifies the work; equal keys must mean identical work
        :param fn: does the work. Gets the job's shared cancellation, which it
            must check instead of any one client's.
        :param cancellation: this caller's own client flag
        """
        with self._lock:
            job = self._jobs.get(key)
            if job is not None and job.group.is_cancelled:
                # That job is winding down; do not attach to a corpse.
                job = None
            leader = job is None
            if leader:
                job = _Job()
                self._jobs[key] = job
            job.group.attach(cancellation)

        if leader:
            return self._lead(key, job, fn)

        log.info("single-flight: attaching to the estimate already running for %s", key)
        return self._follow(key, job, fn, cancellation)

    def _lead(self, key: str, job: _Job, fn: Callable[[GroupCancellation], Any]) -> Any:
        try:
            job.result = fn(job.group)
            return job.result
        except BaseException as e:  # noqa: BLE001 - recorded, then re-raised
            job.error = e
            raise
        finally:
            with self._lock:
                # Only remove our own job: a follower may already have given up on
                # this key and registered a fresh one.
                if self._jobs.get(key) is job:
                    del self._jobs[key]
            job.done.set()

    def _follow(
        self,
        key: str,
        job: _Job,
        fn: Callable[[GroupCancellation], Any],
        cancellation: Optional[Cancellation],
    ) -> Any:
        while not job.done.wait(self._poll_interval):
            raise_if_client_gone(cancellation)

        if job.error is not None:
            if isinstance(job.error, ClientGoneError) and not _is_gone(cancellation):
                # We attached just as the leader stopped for its own clients. Our
                # client is still waiting, so do the work ourselves.
                return self.run(key, fn, cancellation)
            raise job.error
        return job.result


def _is_gone(cancellation: Optional[Cancellation]) -> bool:
    return cancellation is not None and cancellation.is_cancelled
