import asyncio
import json
import re
import threading
import time
from inspect import signature

import pytest
from fastapi import Request

from data_access_service.utils.cancellation import Cancellation, ClientGoneError
from data_access_service.utils.sse_utils import sse_it


def _request(receive=None) -> Request:
    """The request FastAPI injects, with a receive channel the test controls."""

    async def never_disconnects():
        await asyncio.Event().wait()

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [],
        "query_string": b"",
    }
    return Request(scope, receive or never_disconnects)


def _parse_sse_events(body: str) -> list[dict]:
    events = []
    for block in re.split(r"\n\n+", body.strip()):
        if not block:
            continue
        event_type = "message"
        data = None
        for line in block.splitlines():
            if line.startswith("event: "):
                event_type = line[len("event: ") :]
            elif line.startswith("data: "):
                data = json.loads(line[len("data: ") :])
        events.append({"event": event_type, "data": data})
    return events


async def _collect_streaming_response(response) -> str:
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
    return "".join(chunks)


@pytest.mark.asyncio
async def test_sse_it_returns_result_and_closes_stream():
    @sse_it
    def quick_task():
        return {"path": "/tmp/output.pmtiles"}

    response = await quick_task()
    body = await _collect_streaming_response(response)
    events = _parse_sse_events(body)

    assert events[0]["event"] == "processing"
    assert events[0]["data"]["status"] == "processing"
    assert events[-1]["event"] == "result"
    assert events[-1]["data"]["status"] == "completed"
    assert events[-1]["data"]["data"] == {"path": "/tmp/output.pmtiles"}


@pytest.mark.asyncio
async def test_sse_it_sends_periodic_processing_messages():
    @sse_it(interval=0.05)
    def slow_task():
        import time

        time.sleep(0.15)
        return "done"

    response = await slow_task()
    body = await _collect_streaming_response(response)
    events = _parse_sse_events(body)

    processing_events = [e for e in events if e["event"] == "processing"]
    assert len(processing_events) >= 2
    assert events[-1]["data"]["data"] == "done"


@pytest.mark.asyncio
async def test_sse_it_supports_async_functions():
    @sse_it
    async def async_task():
        await asyncio.sleep(0.01)
        return 42

    response = await async_task()
    body = await _collect_streaming_response(response)
    events = _parse_sse_events(body)

    assert events[-1]["data"]["data"] == 42


@pytest.mark.asyncio
async def test_sse_it_emits_error_event_on_failure():
    @sse_it
    def failing_task():
        raise ValueError("boom")

    response = await failing_task()
    body = await _collect_streaming_response(response)
    events = _parse_sse_events(body)

    assert events[-1]["event"] == "error"
    assert events[-1]["data"]["status"] == "error"
    assert "boom" in events[-1]["data"]["message"]


@pytest.mark.asyncio
async def test_sse_it_injects_a_cancellation_when_the_function_asks_for_one():
    seen = {}

    @sse_it
    def task(cancellation=None):
        seen["cancellation"] = cancellation
        return "done"

    await _collect_streaming_response(await task())

    assert isinstance(seen["cancellation"], Cancellation)


@pytest.mark.asyncio
async def test_sse_it_hides_the_cancellation_parameter_from_the_signature():
    # FastAPI builds a route's request fields from the signature, so the
    # injected parameter must not appear there.
    @sse_it
    def task(uuid: str, cancellation=None):
        return uuid

    assert "cancellation" not in signature(task).parameters


@pytest.mark.asyncio
async def test_sse_it_asks_fastapi_for_the_request_it_watches():
    @sse_it
    def task(uuid: str):
        return uuid

    parameters = signature(task).parameters
    assert list(parameters) == ["uuid", "request"]
    assert parameters["request"].annotation is Request


@pytest.mark.asyncio
async def test_sse_it_passes_the_request_on_when_the_function_declares_one():
    # /pmtiles takes a request of its own; it must get that one, not a second.
    seen = {}

    @sse_it
    def task(request: Request):
        seen["request"] = request
        return "done"

    assert list(signature(task).parameters) == ["request"]

    request = _request()
    await _collect_streaming_response(await task(request=request))

    assert seen["request"] is request


@pytest.mark.asyncio
async def test_sse_it_cancels_the_work_when_the_client_disconnects():
    started = threading.Event()
    stopped = threading.Event()
    completed = threading.Event()

    @sse_it(interval=0.01)
    def slow_task(cancellation=None):
        started.set()
        try:
            # Stand-in for the estimate's checkpoints (per key, per fragment).
            for _ in range(500):
                cancellation.raise_if_client_gone()
                time.sleep(0.01)
            completed.set()
            return "done"
        except ClientGoneError:
            stopped.set()
            raise

    response = await slow_task()
    iterator = response.body_iterator
    await iterator.__anext__()  # the first "processing" event
    # The work only starts once the consumer asks for the next event, so pull a
    # heartbeat before pretending the client went away.
    await iterator.__anext__()
    assert started.wait(5)

    # One of the two ways the stream ends: the generator is closed from outside.
    await iterator.aclose()

    assert await _wait_for(stopped, 5), "the worker never reached a checkpoint"
    assert not completed.is_set()


@pytest.mark.asyncio
async def test_sse_it_stops_when_the_receive_channel_reports_a_disconnect():
    """
    The other way, and the one that happens in production.

    Nothing closes the generator there: uvicorn advertises ASGI spec_version
    2.4, so Starlette stops watching for the disconnect itself and trusts send()
    to raise instead - and uvicorn's send() silently drops writes to a client
    that has gone. Polling the receive channel is the only signal left.
    """
    started = threading.Event()
    stopped = threading.Event()
    completed = threading.Event()
    disconnected = threading.Event()

    async def receive():
        # A client that is still there sends nothing; the poll cancels this wait.
        if not disconnected.is_set():
            await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    @sse_it(interval=0.01)
    def slow_task(cancellation=None):
        started.set()
        try:
            for _ in range(500):
                cancellation.raise_if_client_gone()
                time.sleep(0.01)
            completed.set()
            return "done"
        except ClientGoneError:
            stopped.set()
            raise

    response = await slow_task(request=_request(receive))
    iterator = response.body_iterator
    await iterator.__anext__()  # the first "processing" event
    await iterator.__anext__()  # a heartbeat, so the work has started
    assert started.wait(5)

    disconnected.set()

    # Drain it the way Starlette does: the stream has to end on its own, and
    # without a result event nobody is there to read.
    chunks = [chunk async for chunk in iterator]
    assert not [e for e in _parse_sse_events("".join(chunks)) if e["event"] == "result"]

    assert await _wait_for(stopped, 5), "the worker never reached a checkpoint"
    assert not completed.is_set()


async def _wait_for(event: threading.Event, timeout: float) -> bool:
    """Wait on a worker-thread event without blocking the event loop."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if event.is_set():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
async def test_sse_it_does_not_emit_an_error_event_when_the_client_is_gone():
    @sse_it
    def task(cancellation=None):
        raise ClientGoneError("client disconnected")

    response = await task()
    body = await _collect_streaming_response(response)
    events = _parse_sse_events(body)

    assert [e["event"] for e in events] == ["processing"]
    assert not [e for e in events if e["event"] == "error"]
