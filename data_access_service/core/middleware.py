import starlette.middleware.gzip as _gzip_mw
from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware


def configure_gzip_middleware(app: FastAPI) -> None:
    """PNG/WebP tiles are already compressed; gzipping them wastes CPU. Core JSON
    responses that already set their own Content-Encoding (see
    utils/routes_helper.py's gzip_compress) are left untouched by this
    middleware — it skips compression when content-encoding is already set.
    """
    if "image/" not in _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES:
        _gzip_mw.DEFAULT_EXCLUDED_CONTENT_TYPES += ("image/",)  # type: ignore[assignment]
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
