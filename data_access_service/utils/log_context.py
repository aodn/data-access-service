import contextvars
import logging
from contextlib import contextmanager
from types import MappingProxyType
from typing import Iterator, Mapping

# Fields (request_id, job_id) added to every log record in this context.
# Plain threading.Thread workers need contextvars.copy_context().run.
_context: contextvars.ContextVar[Mapping[str, object]] = contextvars.ContextVar(
    "log_context", default=MappingProxyType({})
)


@contextmanager
def bind_log_context(**fields) -> Iterator[None]:
    """Bind log fields for the block; nested binds merge."""
    token = _context.set(MappingProxyType({**_context.get(), **fields}))
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> Mapping[str, object]:
    """Read-only view of the fields currently bound."""
    return _context.get()


class ContextFilter(logging.Filter):
    """Copies bound fields onto each record. Attach to handlers, not loggers,
    so propagated records are covered too."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _context.get().items():
            setattr(record, key, value)
        return True


def install_context_filter(handler: logging.Handler) -> None:
    """Attach ContextFilter to handler unless it already has one."""
    if not any(isinstance(f, ContextFilter) for f in handler.filters):
        handler.addFilter(ContextFilter())
