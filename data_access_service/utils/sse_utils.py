import asyncio
import contextvars
import functools
import logging
import time
from asyncio import CancelledError
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from inspect import Parameter, iscoroutinefunction, signature
from typing import Any, AsyncGenerator, Callable, Optional

from fastapi import Request
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

# The disconnect is read off this request, so sse_it always asks FastAPI for one.
# Polling it is the only signal we get: uvicorn advertises ASGI spec_version 2.4,
# which makes Starlette drop its own disconnect watcher and trust send() to raise
# instead - and uvicorn's send() silently discards writes to a gone client rather
# than raising. Without this poll nothing ever closes the generator below.
REQUEST_KWARG = "request"

# How often the stream checks whether the client is still there.
_DISCONNECT_POLL_INTERVAL: float = 0.1

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


def _declared_parameters(func: Callable) -> set[str]:
    try:
        return set(signature(func).parameters)
    except (TypeError, ValueError):
        return set()


def _public_signature(func: Callable):
    """
    The signature FastAPI sees: cancellation taken out, request put in.

    FastAPI builds a route's request fields from the signature, so the injected
    cancellation has to be hidden or it would be treated as a query parameter.
    The request goes the other way: the stream needs one to watch for the
    disconnect, so ask for it even when the wrapped function does not.
    """
    sig = signature(func)
    parameters = [p for p in sig.parameters.values() if p.name != CANCELLATION_KWARG]
    if REQUEST_KWARG not in sig.parameters:
        # Keyword-only, so it can be appended without disturbing the existing
        # parameters (one of which may well have a default).
        parameters.append(
            Parameter(REQUEST_KWARG, Parameter.KEYWORD_ONLY, annotation=Request)
        )
    return sig.replace(parameters=parameters)


async def _client_gone(request: Optional[Request]) -> bool:
    """
    Has the client hung up? Peeks at the receive channel, never waits.

    False when there is no request - a direct call from a test or a batch job has
    no client that can go away.
    """
    if request is None:
        return False
    return await request.is_disconnected()


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

    The stream polls the request while it waits, and stops as soon as the client
    disconnects. If the wrapped function declares a `cancellation` parameter it
    is given one, set at that moment, so it can stop itself too.
    """

    def decorator(fn: Callable):
        declared = _declared_parameters(fn)
        injects_cancellation = CANCELLATION_KWARG in declared
        passes_request = REQUEST_KWARG in declared

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            # FastAPI calls the endpoint with keyword arguments only.
            request = kwargs.get(REQUEST_KWARG)
            if not passes_request:
                kwargs.pop(REQUEST_KWARG, None)

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
                            if await _client_gone(request):
                                # Return rather than break: there is no result to
                                # send, and nobody to send it to. The finally
                                # below stops the work.
                                logger.info(
                                    "[%s] client disconnected, stopping.", fn.__name__
                                )
                                return
                            if time.time() - last_sent_sse >= interval:
                                yield format_sse(
                                    {
                                        STATUS: "processing",
                                        MESSAGE: "Still processing...",
                                    },
                                    "processing",
                                )
                                last_sent_sse = time.time()
                            await asyncio.sleep(_DISCONNECT_POLL_INTERVAL)

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

        wrapper.__signature__ = _public_signature(fn)

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
