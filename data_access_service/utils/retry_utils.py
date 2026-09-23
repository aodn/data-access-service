"""Shared pieces for the tenacity retries used across the service.

Every retry site wants the same log line before it sleeps -- which attempt
failed, why, and how long until the next one -- so they all build it here
instead of each writing its own callback.
"""

import logging
from typing import Callable

log = logging.getLogger(__name__)

# Below this, a wait reads more naturally in seconds than in minutes.
_MINUTES_THRESHOLD_SECONDS = 120


def format_wait(seconds: float) -> str:
    """Render a retry wait as seconds or minutes, whichever reads better."""
    if seconds < _MINUTES_THRESHOLD_SECONDS:
        return f"{seconds:.0f}s"
    return f"{round(seconds / 60, 1)} minute(s)"


def log_retry_attempt(
    operation: str, logger: logging.Logger | None = None
) -> Callable[..., None]:
    """Build a tenacity ``before_sleep`` callback that logs the failed attempt.

    ``operation`` names what was being retried, e.g. "DuckDB S3 read". Pass the
    calling module's ``logger`` so the record keeps that module's name.
    """
    target = logger if logger is not None else log

    def _before_sleep(retry_state) -> None:
        attempt = retry_state.attempt_number
        target.warning(
            "[Retry] %s failed on attempt #%s.\n"
            "Error details: %s\n"
            "Waiting %s before attempt #%s...",
            operation,
            attempt,
            retry_state.outcome.exception(),
            format_wait(retry_state.next_action.sleep),
            attempt + 1,
        )

    return _before_sleep
