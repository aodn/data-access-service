import asyncio
import contextvars
import functools
import logging
import time
from asyncio import CancelledError
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from inspect import iscoroutinefunction, signature
from typing import Any, AsyncGenerator, Callable, Optional

from fastapi.responses import StreamingResponse

from data_access_service.core.constants import DATA, MESSAGE, SSE_WORKER_THREADS, STATUS
from data_access_service.utils.cancellation import Cancellation, ClientGoneError
from data_access_service.utils.sse_wrapper import format_sse

logger = logging.getLogger(__name__)

SSE_IT_INTERVAL: float = 20.0

# A wrapped function that declares this parameter gets the request's Cancellation
# passed in. It is hidden from FastAPI (see _public_signature), so it never shows
# up as a request field.
CANCELLATION_KWARG = "cancellation"

# Own pool instead of the default asyncio executor, so a queue of estimates
# cannot starve every other to_thread caller in the service.
_executor = ThreadPoolExecutor(
    max_workers=SSE_WORKER_THREADS, thread_name_prefix="sse-stream"
)


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


def _accepts_cancellation(func: Callable) -> bool:
    try:
        return CANCELLATION_KWARG in signature(func).parameters
    except (TypeError, ValueError):
        return False


def _public_signature(func: Callable):
    """
    The wrapped function's signature without the cancellation parameter.

    FastAPI builds a route's request fields from the signature, so the injected
    parameter has to be hidden or it would be treated as a query parameter.
    """
    sig = signature(func)
    return sig.replace(
        parameters=[p for p in sig.parameters.values() if p.name != CANCELLATION_KWARG]
    )


async def _run_wrapped_function(func: Callable, *args, **kwargs) -> Any:
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)
    loop = asyncio.get_running_loop()
    # copy_context() is what asyncio.to_thread does for us; keep it so anything
    # context-local still resolves inside the worker thread.
    ctx = contextvars.copy_context()
    return await loop.run_in_executor(
        _executor, functools.partial(ctx.run, func, *args, **kwargs)
    )


def _drop_task(task: asyncio.Task) -> None:
    """
    Stop awaiting the task and make sure its outcome is never left unretrieved.

    Cancelling only stops the coroutine waiting on the worker thread - the thread
    itself keeps running until it reaches a cancellation checkpoint.
    """
    if not task.done():
        task.cancel()
    elif not task.cancelled():
        # Retrieve it so asyncio does not log "exception was never retrieved".
        task.exception()


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

    If the wrapped function declares a `cancellation` parameter it is given one,
    set when the client disconnects, so it can stop itself.
    """

    def decorator(fn: Callable):
        injects_cancellation = _accepts_cancellation(fn)

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            async def sse_stream() -> AsyncGenerator[str, None]:
                cancellation = Cancellation()
                if injects_cancellation:
                    kwargs[CANCELLATION_KWARG] = cancellation
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

                    try:
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
                    finally:
                        # Runs when the client disconnects, on failure, and on
                        # success. On success the work is already done, so this
                        # only tidies up.
                        cancellation.cancel()
                        _drop_task(task)

                except CancelledError:
                    logger.debug("[%s] SSE stream cancelled.", fn.__name__)
                    raise

                except ClientGoneError:
                    # No error event: nobody is listening, and on the ogcapi-java
                    # side it would be indistinguishable from a real failure.
                    logger.info("[%s] stopped, client gone.", fn.__name__)

                except Exception as e:
                    logger.error("[%s] SSE stream failed: %s", fn.__name__, str(e))
                    yield format_sse({STATUS: "error", MESSAGE: str(e)}, "error")

            return StreamingResponse(
                sse_stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
            )

        if injects_cancellation:
            wrapper.__signature__ = _public_signature(fn)

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
