import asyncio
import json
import time
from fastapi.responses import StreamingResponse


# Helper function to format SSE messages
def format_sse(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# SSE Wrapper function with periodic processing messages, it accepts a function
# which is the function that you want to execute and generate result
#
# Then this wrapper just send processing message out via SSE and then
# when the function call completed, it attach the result to the last
# SSE message and terminate connection. So any function can warp with
# this function to get SSE support
async def sse_wrapper(async_function, *function_args):
    async def sse_stream():
        try:
            # Send initial processing message
            yield format_sse(
                {"status": "processing", "message": "Processing your request..."},
                "processing",
            )

            # Track start time for periodic messages
            start_time = time.time()
            processing_interval = 20  # Send processing message every 20 seconds

            # Execute the async function in the background
            task = asyncio.create_task(async_function(*function_args))

            # Send periodic processing messages while the task is running
            while not task.done():
                await asyncio.sleep(1)  # Check every second to avoid busy-waiting
                if time.time() - start_time >= processing_interval:
                    yield format_sse(
                        {"status": "processing", "message": "Still processing..."},
                        "processing",
                    )
                    start_time = time.time()  # Reset timer

            # Get the result of the async function
            result = await task

            # Send the result
            yield format_sse({"status": "completed", "data": result}, "result")
        except Exception as e:
            # Send error message
            yield format_sse({"status": "error", "message": str(e)}, "error")

    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
