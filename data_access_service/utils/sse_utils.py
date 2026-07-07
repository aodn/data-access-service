import asyncio
import logging
import time
from asyncio import CancelledError
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, AsyncGenerator, Callable, Optional

from fastapi.responses import StreamingResponse

from data_access_service.core.constants import DATA, MESSAGE, STATUS
from data_access_service.utils.sse_wrapper import format_sse

logger = logging.getLogger(__name__)

SSE_IT_INTERVAL: float = 30.0


def _to_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return list(value)
    return str(value)


async def _run_wrapped_function(func: Callable, *args, **kwargs) -> Any:
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)
    return await asyncio.to_thread(func, *args, **kwargs)


def sse_it(
    func: Optional[Callable] = None,
    *,
    interval: float = SSE_IT_INTERVAL,
):
    """
    Decorator (like time_it) that wraps a function and streams its result over SSE.

    Sends an initial processing event, then periodic processing heartbeats every
    `interval` seconds while the wrapped function runs. When the function completes,
    sends the return value in a final result event and closes the stream.
    """

    def decorator(fn: Callable):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            async def sse_stream() -> AsyncGenerator[str, None]:
                try:
                    yield format_sse(
                        {
                            STATUS: "processing",
                            MESSAGE: "Processing your request...",
                        },
                        "processing",
                    )

                    task = asyncio.create_task(
                        _run_wrapped_function(fn, *args, **kwargs)
                    )

                    last_sent_sse = time.time()
                    while not task.done():
                        if time.time() - last_sent_sse >= interval:
                            yield format_sse(
                                {
                                    STATUS: "processing",
                                    MESSAGE: "Still processing...",
                                },
                                "processing",
                            )
                            last_sent_sse = time.time()
                        await asyncio.sleep(0.1)

                    result = task.result()
                    yield format_sse(
                        {
                            STATUS: "completed",
                            MESSAGE: "Done",
                            DATA: _to_json_safe(result),
                        },
                        "result",
                    )
                    logger.debug("[%s] SSE stream completed.", fn.__name__)

                except CancelledError:
                    logger.debug("[%s] SSE stream cancelled.", fn.__name__)
                    raise

                except Exception as e:
                    logger.error("[%s] SSE stream failed: %s", fn.__name__, str(e))
                    yield format_sse({STATUS: "error", MESSAGE: str(e)}, "error")

            return StreamingResponse(
                sse_stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
            )

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
