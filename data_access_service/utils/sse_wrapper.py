import json
import time
import asyncio
from asyncio import CancelledError
from typing import AsyncGenerator, Callable, Generator, Any

from fastapi.responses import StreamingResponse
from data_access_service.core.constants import STATUS, MESSAGE, DATA

# debugging purpose, will use application logger later.
import logging

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE: int = 5000
HEART_BEAT_INTERVAL: float = 10.0


# Helper function to format SSE messages
def format_sse(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def split_list(lst, chunk_size=DEFAULT_CHUNK_SIZE) -> Generator[Any, Any, None]:
    """
    Split a list into chunks of specified size.
    :return: Generator to reduce memory usage.
    """
    return (lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size))


async def _collect_records(
    async_function: Callable[..., AsyncGenerator[dict, None]],
    function_args: tuple,
) -> list:
    """
    Collect all records from the async generator into a list.
    This allows the data fetching to run as an independent asyncio task,
    so a heartbeat loop can run concurrently while data is being fetched.

    We explicitly close the generator in finally to guarantee that any
    resource cleanup (e.g. memory_lock release inside fetch_data) happens,
    even in error or cancellation scenarios.
    """
    result = []
    agen = async_function(*function_args)
    try:
        async for record in agen:
            result.append(record)
    finally:
        try:
            await agen.aclose()
        except Exception:
            pass  # best effort
    return result


async def sse_wrapper(
    request_id: str,
    async_function: Callable[..., AsyncGenerator[dict, None]],
    *function_args,
):
    """
    SSE Wrapper function that streams results from an async generator over SSE.

    Key behavior:
    - Sends an initial 'processing' message immediately on connection.
    - Runs the data fetching function as an independent asyncio task.
    - While the task is running, sends periodic heartbeat messages to keep
      the connection alive. This prevents ALB / reverse proxies from closing
      the connection during long-running data fetches (> 30s) before the
      first data record is produced.
    - Once the task completes, streams the collected records in chunks.
    - Sends a final chunk with '/end' suffix so the caller knows all data
      has been delivered.

    :param request_id: Unique ID for debug tracing
    :param async_function: Async generator function that produces data records
    :param function_args: Arguments to pass to async_function
    """
    # Processing interval for periodic messages
    processing_interval: float = (
        HEART_BEAT_INTERVAL  # Send processing message every 10 seconds
    )
    chunk_size: int = DEFAULT_CHUNK_SIZE * 2  # Number of records consume before chunks

    async def sse_stream():
        try:
            # debug the sse process
            logger.debug(
                "SSE started the initial processing, request_id=%s", request_id
            )

            # Send initial processing message
            yield format_sse(
                {
                    STATUS: "processing",
                    MESSAGE: "Processing your request...",
                    "request_id": request_id,
                },
                "processing",
            )

            # Run the data fetching as an independent asyncio task to allow heartbeat loop below to run concurrently
            # instead of being blocked waiting for the first record.
            def collect_sync():
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(
                        _collect_records(async_function, function_args)
                    )
                finally:
                    loop.close()

            task = asyncio.create_task(asyncio.to_thread(collect_sync))

            # Heartbeat loop: runs while the data fetch task is in progress.
            # Sends a 'processing' message every processing_interval seconds.
            # This keeps the SSE connection alive through long data fetch operations
            # and prevents intermediate proxies from timing out the connection.
            last_sent_sse = time.time()
            while not task.done():
                if time.time() - last_sent_sse >= processing_interval:
                    yield format_sse(
                        {
                            STATUS: "processing",
                            MESSAGE: "Still processing...",
                            "request_id": request_id,
                        },
                        "processing",
                    )
                    logger.debug("SSE heartbeat sent, request_id=%s", request_id)
                    last_sent_sse = time.time()
                # Small sleep to prevent busy-waiting while still checking task completion frequently enough.
                await asyncio.sleep(0.1)

            # Task is done — retrieve the result.
            # task.result() will re-raise any exception that occurred inside the task.
            all_records = task.result()

            # Stream collected records in chunks
            chunk = []
            chunk_count = 0

            for record in all_records:
                # None signals no more data from the generator
                if record is None:
                    break

                chunk.append(record)

                # When the chunk buffer is full, flush it as an SSE event
                if len(chunk) >= chunk_size:
                    for small_chunk in split_list(chunk, chunk_size):
                        chunk_count += 1
                        yield format_sse(
                            {
                                STATUS: "completed",
                                MESSAGE: f"chunk {chunk_count}",
                                DATA: small_chunk,
                            },
                            "result",
                        )
                    chunk = []

            # Flush any remaining records in the buffer.
            # The '/end' suffix in the message tells the caller that all data
            # has been sent and the stream is complete.
            if chunk:
                for small_chunk in split_list(chunk, chunk_size):
                    chunk_count += 1
                    yield format_sse(
                        {
                            STATUS: "completed",
                            MESSAGE: f"chunk {chunk_count}/end",
                            DATA: small_chunk,
                        },
                        "result",
                    )
            else:
                yield format_sse(
                    {
                        STATUS: "completed",
                        MESSAGE: "chunk 0/end",
                        DATA: [],
                    },
                    "result",
                )
            logger.debug("SSE request completed, request_id=%s", request_id)

        except CancelledError:
            logger.debug("SSE request cancelled, request_id=%s", request_id)
            raise

        except Exception as e:
            logger.error(
                "SSE request failed, request_id=%s, error=%s", request_id, str(e)
            )
            yield format_sse({STATUS: "error", MESSAGE: str(e)}, "error")

    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
