import asyncio
import contextvars
import json
import logging
import re
import threading
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from data_access_service.core.middleware import (
    RequestContextMiddleware,
    configure_request_context_middleware,
)
from data_access_service.core.routes import helpers as route_helpers
from data_access_service.utils.log_context import (
    ContextFilter,
    bind_log_context,
    current_context,
    install_context_filter,
)
from data_access_service.utils.log_formatter import JsonLogFormatter
from data_access_service.utils.sse_wrapper import sse_wrapper

UUID4 = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class _ListHandler(logging.Handler):
    """Collects formatted JSON lines, with ContextFilter attached like the real
    handlers from log_config.yaml / init_log."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.setFormatter(JsonLogFormatter())
        install_context_filter(self)
        self.lines = []

    def emit(self, record):
        self.lines.append(self.format(record))

    def payloads(self):
        return [json.loads(line) for line in self.lines]


@pytest.fixture
def captured():
    """Attach a capturing handler to root at DEBUG; restore afterwards."""
    root = logging.getLogger()
    old_level = root.level
    handler = _ListHandler()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    yield handler
    root.removeHandler(handler)
    root.setLevel(old_level)


def _by_message(handler, message):
    matches = [p for p in handler.payloads() if p["message"] == message]
    assert matches, f"no log line with message {message!r}"
    return matches


# -- bind_log_context / current_context ----------------------------------------


def test_bind_log_context_nests_and_restores():
    assert dict(current_context()) == {}
    with bind_log_context(request_id="r1"):
        assert dict(current_context()) == {"request_id": "r1"}
        with bind_log_context(job_id="j1"):
            assert dict(current_context()) == {"request_id": "r1", "job_id": "j1"}
        with bind_log_context(request_id="r2"):
            assert dict(current_context()) == {"request_id": "r2"}
        assert dict(current_context()) == {"request_id": "r1"}
    assert dict(current_context()) == {}


def test_bind_log_context_restores_on_exception():
    with pytest.raises(RuntimeError):
        with bind_log_context(request_id="boom"):
            raise RuntimeError()
    assert dict(current_context()) == {}


def test_current_context_is_read_only():
    with bind_log_context(request_id="r1"):
        with pytest.raises(TypeError):
            current_context()["request_id"] = "tampered"


# -- ContextFilter --------------------------------------------------------------


def test_context_filter_sets_bound_fields_on_record():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "m", None, None)
    with bind_log_context(request_id="r1", job_id="j1"):
        assert ContextFilter().filter(record) is True
    assert record.request_id == "r1"
    assert record.job_id == "j1"


def test_context_filter_adds_nothing_when_unbound(captured):
    logging.getLogger("das.test.ctx").info("unbound")
    payload = _by_message(captured, "unbound")[0]
    assert "request_id" not in payload and "job_id" not in payload


def test_install_context_filter_is_idempotent():
    handler = logging.StreamHandler()
    install_context_filter(handler)
    install_context_filter(handler)
    assert sum(isinstance(f, ContextFilter) for f in handler.filters) == 1


def test_bound_fields_are_top_level_json(captured):
    with bind_log_context(request_id="r-json"):
        logging.getLogger("das.test.ctx").warning("bound line")
    assert _by_message(captured, "bound line")[0]["request_id"] == "r-json"


# -- Risk 1: propagation across asyncio.to_thread + a nested event loop --------


@pytest.mark.asyncio
async def test_context_survives_to_thread_plus_nested_event_loop(captured):
    """The bare primitive sse_wrapper relies on: to_thread copies the context,
    and the Task run_until_complete creates on the fresh loop copies it again."""
    log = logging.getLogger("das.test.nested")

    async def inner():
        await asyncio.sleep(0)
        log.info("inner coroutine")
        # A task spawned inside the nested loop inherits it too.
        await asyncio.create_task(asyncio.sleep(0))
        log.info("after nested task")

    def collect_sync():
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(inner())
        finally:
            loop.close()

    with bind_log_context(request_id="nested-1"):
        await asyncio.create_task(asyncio.to_thread(collect_sync))

    for message in ("inner coroutine", "after nested task"):
        payload = _by_message(captured, message)[0]
        assert payload["request_id"] == "nested-1"
        assert payload["threadId"] != threading.get_ident()


@pytest.mark.asyncio
async def test_sse_wrapper_propagates_request_id_into_fetch_generator(captured):
    """Drives the real sse_wrapper: logs from inside the async generator it runs
    on a worker thread + fresh event loop must carry the bound request_id, and
    the SSE 'processing' event still returns it to the caller."""
    log = logging.getLogger("das.test.fetch")

    async def fake_fetch(n):
        log.info("fetch started")
        for i in range(n):
            await asyncio.sleep(0)
            yield {"i": i}
        log.info("fetch finished")

    # Starlette streams the body inside the request's context (the middleware
    # task), so consume it inside the bind here too.
    with bind_log_context(request_id="sse-req-1"):
        response = await sse_wrapper(fake_fetch, 3)
        body = "".join([chunk async for chunk in response.body_iterator])

    events = [
        json.loads(line[len("data: ") :])
        for line in body.splitlines()
        if line.startswith("data: ")
    ]
    assert events[0]["request_id"] == "sse-req-1"
    assert events[-1]["message"] == "chunk 1/end"
    assert [r["i"] for r in events[-1]["data"]] == [0, 1, 2]

    for message in ("fetch started", "fetch finished"):
        assert _by_message(captured, message)[0]["request_id"] == "sse-req-1"
    sse_lines = [
        p for p in captured.payloads() if p["loggerName"].endswith("sse_wrapper")
    ]
    assert sse_lines
    assert all(p["request_id"] == "sse-req-1" for p in sse_lines)


@pytest.mark.asyncio
async def test_generator_body_runs_in_the_consumers_context(captured):
    """An async generator does not capture the context it was created in; its
    body sees whatever context iterates it. That is why RequestContextMiddleware
    must cover the whole response, streaming included - binding only around the
    handler call would not reach an SSE body."""
    log = logging.getLogger("das.test.gen")

    async def gen():
        log.info("generator body")
        yield 1

    with bind_log_context(request_id="created-here"):
        agen = gen()
    assert [x async for x in agen] == [1]

    assert "request_id" not in _by_message(captured, "generator body")[0]


def test_plain_thread_does_not_inherit_context_without_copy(captured):
    """Negative control: why async_response_json wraps its thread target in
    copy_context().run. If this starts failing, Python changed thread context
    inheritance and that wrapper may be redundant."""
    log = logging.getLogger("das.test.thread")
    with bind_log_context(request_id="lost"):
        t = threading.Thread(target=lambda: log.info("bare thread"))
        t.start()
        t.join()
        ctx = contextvars.copy_context()
        t = threading.Thread(target=ctx.run, args=(log.info, "copied thread"))
        t.start()
        t.join()
    assert "request_id" not in _by_message(captured, "bare thread")[0]
    assert _by_message(captured, "copied thread")[0]["request_id"] == "lost"


def test_async_response_json_propagates_request_id_into_worker_thread(captured):
    log = logging.getLogger("das.test.nonsse")

    async def fake_fetch():
        log.info("non-sse fetch")
        yield {"a": 1}
        yield None

    with bind_log_context(request_id="plain-req"):
        response = route_helpers.async_response_json(fake_fetch(), compress=False)

    assert json.loads(response.body) == [{"a": 1}]
    assert _by_message(captured, "non-sse fetch")[0]["request_id"] == "plain-req"


# -- RequestContextMiddleware (BaseHTTPMiddleware boundary) -----------------------


def _mini_app():
    app = FastAPI()
    configure_request_context_middleware(app)
    log = logging.getLogger("das.test.route")

    @app.get("/sync")
    def sync_route():
        # sync handlers run in the threadpool (anyio.to_thread)
        log.info("sync handler")
        return current_context().get("request_id")

    @app.get("/async")
    async def async_route():
        log.info("async handler")
        await asyncio.sleep(0)
        return current_context().get("request_id")

    @app.get("/stream")
    async def stream_route():
        async def body():
            for i in range(3):
                await asyncio.sleep(0)
                log.info("streaming chunk %d", i)
                yield f"{i}\n"

        return StreamingResponse(body(), media_type="text/plain")

    @app.get("/boom")
    async def boom_route():
        raise RuntimeError("handler failed")

    return app


def test_middleware_binds_one_request_id_per_request(captured):
    client = TestClient(_mini_app())

    first = client.get("/async").json()
    second = client.get("/sync").json()

    assert UUID4.match(first) and UUID4.match(second)
    assert first != second
    assert _by_message(captured, "async handler")[0]["request_id"] == first
    assert _by_message(captured, "sync handler")[0]["request_id"] == second
    # Nothing leaks back into the caller's context once the request is done.
    assert dict(current_context()) == {}


def test_middleware_request_id_reaches_streaming_body(captured):
    client = TestClient(_mini_app())

    assert client.get("/stream").text == "0\n1\n2\n"

    ids = {
        p.get("request_id")
        for p in captured.payloads()
        if p["message"].startswith("streaming chunk")
    }
    assert len(ids) == 1
    assert UUID4.match(ids.pop())


def test_middleware_request_id_on_unhandled_exception_log(captured):
    client = TestClient(_mini_app(), raise_server_exceptions=False)
    assert client.get("/boom").status_code == 500
    # Starlette's ServerErrorMiddleware sits outside user middleware, so its
    # traceback log is outside the bound context; the middleware must not have
    # swallowed the error or left the context bound.
    assert dict(current_context()) == {}


def test_server_app_registers_request_context_middleware():
    from data_access_service.server import app

    assert any(m.cls is RequestContextMiddleware for m in app.user_middleware)


# -- The real /data route, end to end --------------------------------------------


@pytest.fixture
def data_client(monkeypatch):
    """The real server app with auth/readiness stubbed and fetch_data replaced
    by a generator that logs from 'deep' inside the fetch path."""
    from data_access_service.core.routes import data as data_routes
    from data_access_service.core.routes.auth import api_key_auth
    from data_access_service.core.routes.helpers import require_api_ready
    from data_access_service.server import app

    log = logging.getLogger("das.test.deep")

    async def fake_fetch(*_args):
        log.info("deep fetch")
        yield {"TIME": "2020-01-01"}
        yield None

    monkeypatch.setattr(data_routes, "fetch_data", fake_fetch)
    app.dependency_overrides[api_key_auth] = lambda: "key"
    app.dependency_overrides[require_api_ready] = lambda: MagicMock()
    # No lifespan: we only need routing + middleware.
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.mark.parametrize("fmt", ["json", "sse/json"])
def test_data_route_request_id_reaches_deep_fetch_logs(fmt, data_client, captured):
    response = data_client.get(
        "/api/v1/das/data/some-uuid/some-key.parquet",
        params={
            "f": fmt,
            "start_date": "2020-01-01",
            "end_date": "2020-01-02 00:00:00.000000000",
        },
    )
    assert response.status_code == 200

    # data.py logs through init_log()'s "data_access_service" logger
    route_ids = {
        p.get("request_id")
        for p in captured.payloads()
        if p["message"] in ("Receiving request", "SSE request started")
        or p["message"] == "Not a SSE request"
    }
    deep = _by_message(captured, "deep fetch")[0]

    assert len(route_ids) == 1
    request_id = route_ids.pop()
    assert UUID4.match(request_id)
    assert deep["request_id"] == request_id
    if fmt.startswith("sse/"):
        assert f'"request_id": "{request_id}"' in response.text
