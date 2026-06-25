import asyncio
import json
import re

import pytest

from data_access_service.utils.sse_utils import sse_it


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
