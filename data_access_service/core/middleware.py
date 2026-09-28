import uuid

import starlette.middleware.gzip as _gzip_mw
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request

from data_access_service.utils.log_context import bind_log_context


def configure_gzip_middleware(app: FastAPI) -> None:
    """PNG/WebP tiles are already compressed; gzipping them wastes CPU. Core JSON
    responses that already set their own Content-Encoding (see
    core/routes/helpers.py's gzip_compress) are left untouched by this
    middleware — it skips compression when content-encoding is already set.
    """
    if "image/" not in _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES:
        _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES += ("image/",)  # type: ignore[assignment]
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Binds a fresh request_id for the whole request, so every log line it
    produces (route, fetch path, SSE worker thread) carries the same value."""

    async def dispatch(self, request: Request, call_next):
        with bind_log_context(request_id=str(uuid.uuid4())):
            return await call_next(request)


def configure_request_context_middleware(app: FastAPI) -> None:
    app.add_middleware(RequestContextMiddleware)
