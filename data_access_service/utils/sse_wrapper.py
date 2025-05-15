import asyncio
import json
import time
import threading
import queue
from fastapi.responses import StreamingResponse


# Helper function to format SSE messages
def format_sse(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def split_list(lst, chunk_size=50000):
    """Split a list into chunks of specified size."""
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


# SSE Wrapper function with periodic processing messages, it accepts a function
# which is the function that you want to execute and generate result
#
# Then this wrapper just send processing message out via SSE and then
# when the function call completed, it attach the result to the last
# SSE message and terminate connection. So any function can warp with
# this function to get SSE support
async def sse_wrapper(async_function, *function_args):
    async def sse_stream():
        # Thread-safe event to signal task completion
        task_done_event = threading.Event()
        # Thread-safe queue to pass task result or exception
        result_queue = queue.Queue()
        # Processing interval for periodic messages
        processing_interval: float = 20.0  # Send processing message every 20 seconds

        def run_new_event_loop():
            # Create a new event loop in this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Create and run the task in the new event loop
                task = loop.create_task(async_function(*function_args))
                loop.run_until_complete(task)
                # Put the result in the queue
                result_queue.put(("success", task.result()))
            except Exception as e:
                # Put the exception in the queue
                result_queue.put(("error", str(e)))
            finally:
                task_done_event.set()  # Signal task completion
                loop.close()  # Clean up the event loop

        try:
            # Start the new event loop in a separate thread
            thread = threading.Thread(target=run_new_event_loop, daemon=True)
            thread.start()

            # Send initial processing message
            yield format_sse(
                {"status": "processing", "message": "Processing your request..."},
                "processing",
            )

            # Track start time for periodic messages
            start_time = time.time()

            # Send periodic processing messages while the task is running
            while not task_done_event.is_set():
                await asyncio.sleep(1)  # Non-blocking sleep in the main event loop
                if time.time() - start_time >= processing_interval:
                    yield format_sse(
                        {"status": "processing", "message": "Still processing..."},
                        "processing",
                    )
                    start_time = time.time()  # Reset timer

            # Wait for the task to complete and get the result from the queue
            thread.join()  # Ensure the thread has finished
            status, value = result_queue.get_nowait()
            if status == "success":
                if type(value) is list:
                    # If it is a list, try to split it if too many lines there
                    smaller_list = split_list(value)
                    for i, chunk in enumerate(smaller_list):
                        yield format_sse(
                            {
                                "status": "completed",
                                "message": str(i + 1) + "/" + str(len(smaller_list)),
                                "data": chunk,
                            },
                            "result",
                        )
                else:
                    yield format_sse({"status": "completed", "data": value}, "result")
            else:
                yield format_sse({"status": "error", "message": value}, "error")

        except Exception as e:
            # Handle errors in the main event loop (e.g., queue issues)
            yield format_sse({"status": "error", "message": str(e)}, "error")
            task_done_event.set()  # Ensure messaging stops

    return StreamingResponse(
        sse_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
