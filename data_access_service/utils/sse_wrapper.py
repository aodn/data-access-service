import asyncio
import json
import time
import threading
import queue
from typing import AsyncGenerator, Callable, Awaitable

from fastapi.responses import StreamingResponse


# Helper function to format SSE messages
def format_sse(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def split_list(lst, chunk_size=50000):
    """Split a list into chunks of specified size."""
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


# SSE Wrapper function with periodic processing messages, it accepts a function of return type AsyncGenerator[dict, None]
# which is the function that you want to execute and generate result with yield (Generator) return, this help to
# reduce the memory use (aka avoid big list)
#
# Then this wrapper just send processing message out via SSE and then
# when the function call completed, it attach the result to the last
# SSE message and terminate connection. So any function can warp with
# this function to get SSE support
async def sse_wrapper(
    async_function: Callable[..., AsyncGenerator[dict, None]], *function_args
):
    async def sse_stream():
        # Processing interval for periodic messages
        processing_interval: float = 20.0  # Send processing message every 20 seconds
        chunk_size: int = 50000  # Number of records per chunk, matching split_list

        try:
            # Send initial processing message
            yield format_sse(
                {"status": "processing", "message": "Processing your request..."},
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
                                "status": "completed",
                                "message": f"chunk {chunk_count}",
                                "data": small_chunk,
                            },
                            "result",
                        )
                    chunk = []  # Reset chunk

                # Send periodic processing message
                if time.time() - start_time >= processing_interval:
                    yield format_sse(
                        {"status": "processing", "message": "Still processing..."},
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
                            "status": "completed",
                            "message": f"chunk {chunk_count}/end",
                            "data": small_chunk,
                        },
                        "result",
                    )

        except Exception as e:
            yield format_sse({"status": "error", "message": str(e)}, "error")

    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
