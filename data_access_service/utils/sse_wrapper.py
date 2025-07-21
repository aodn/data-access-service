import json
import time
from asyncio import CancelledError
from typing import AsyncGenerator, Callable, Generator, Any

from fastapi.responses import StreamingResponse
from data_access_service.core.constants import STATUS, MESSAGE, DATA

DEFAULT_CHUNK_SIZE: int = 10000


# Helper function to format SSE messages
def format_sse(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def split_list(lst, chunk_size=DEFAULT_CHUNK_SIZE) -> Generator[Any, Any, None]:
    """
    Split a list into chunks of specified size.
    :return: Generator to reduce memory usage.
    """
    return (lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size))


async def sse_wrapper(
    async_function: Callable[..., AsyncGenerator[dict, None]], *function_args
):
    """
    SSE Wrapper function with periodic processing messages, it accepts a function of return type AsyncGenerator[dict, None]
    which is the function that you want to execute and generate result with yield (Generator) return, this help to
    reduce the memory use (aka avoid big list)

    Then this wrapper just send processing message out via SSE when the function call completed, it attached
    the result to the last SSE message and terminate connection. So any function can warp with
    this function to get SSE support

    :param async_function:
    :param function_args:
    :return:
    """

    async def sse_stream():
        # Processing interval for periodic messages
        processing_interval: float = 20.0  # Send processing message every 20 seconds
        chunk_size: int = (
            DEFAULT_CHUNK_SIZE  # Number of records per chunk, matching split_list
        )

        try:
            # Send initial processing message
            yield format_sse(
                {STATUS: "processing", MESSAGE: "Processing your request..."},
                "processing",
            )
            start_time = time.time()

            # Initialize chunk buffer
            chunk = []
            chunk_count = 0
            # Iterate over the async generator
            async for record in async_function(*function_args):
                chunk.append(record)

                # When chunk reaches chunk_size, split and yield
                if len(chunk) >= chunk_size:
                    chunks = split_list(chunk, chunk_size)
                    for i, small_chunk in enumerate(chunks):
                        chunk_count += 1
                        yield format_sse(
                            {
                                STATUS: "completed",
                                MESSAGE: f"chunk {chunk_count}",
                                DATA: small_chunk,
                            },
                            "result",
                        )
                    chunk = []  # Reset chunk

                # Send periodic processing message
                if time.time() - start_time >= processing_interval:
                    yield format_sse(
                        {STATUS: "processing", MESSAGE: "Still processing..."},
                        "processing",
                    )
                    start_time = time.time()

            # Yield any remaining records
            if chunk:
                chunks = split_list(chunk, chunk_size)
                for i, small_chunk in enumerate(chunks):
                    chunk_count += 1
                    yield format_sse(
                        {
                            STATUS: "completed",
                            MESSAGE: f"chunk {chunk_count}/end",
                            DATA: small_chunk,
                        },
                        "result",
                    )

        except CancelledError as ge:
            raise

        except Exception as e:
            yield format_sse({STATUS: "error", MESSAGE: str(e)}, "error")

    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
