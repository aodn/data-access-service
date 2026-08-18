"""
Cooperative cancellation for work started by an SSE request.

The estimate runs in a worker thread, and Python cannot kill a thread from the
outside. So cancellation has to be cooperative: sse_it sets a flag when the
client disconnects, and the work checks that flag at a few checkpoints and stops
itself.
"""

import threading
from typing import Optional


class ClientGoneError(Exception):
    """
    Raised at a checkpoint once the SSE client has disconnected.

    Deliberately NOT a ValueError: estimate_datasets_size catches ValueError per
    key to skip keys that cannot produce the requested format, and a cancellation
    must not be swallowed there.
    """


class Cancellation:
    """One client's disconnect flag: set on the event loop, read from a worker thread."""

    def __init__(self) -> None:
        # threading.Event rather than a bool or an asyncio primitive, because it
        # is written and read from different threads.
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        self._cancelled.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    def raise_if_client_gone(self) -> None:
        if self.is_cancelled:
            raise ClientGoneError("client disconnected")


class GroupCancellation(Cancellation):
    """
    The flag a single-flight job checks, shared by every client attached to it.

    Cancelled only once EVERY attached client has gone: the first client to leave
    must not stop work the others are still waiting for.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._clients: list[Optional[Cancellation]] = []

    def attach(self, client: Optional[Cancellation]) -> None:
        with self._lock:
            self._clients.append(client)

    @property
    def is_cancelled(self) -> bool:
        if self._cancelled.is_set():
            return True
        with self._lock:
            # A caller with no cancellation (batch job, test) never goes away, so
            # its presence keeps the job alive.
            return bool(self._clients) and all(
                client is not None and client.is_cancelled for client in self._clients
            )


def raise_if_client_gone(cancellation: Optional[Cancellation]) -> None:
    """Checkpoint helper: does nothing when there is no cancellation to check."""
    if cancellation is not None:
        cancellation.raise_if_client_gone()
