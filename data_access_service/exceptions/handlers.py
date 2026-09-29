import logging

import duckdb
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


async def file_not_found_handler(request: Request, exc: FileNotFoundError):
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def duckdb_out_of_memory_handler(
    request: Request, exc: duckdb.OutOfMemoryException
):
    logger.warning("DuckDB out of memory on %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={"detail": "Server is busy, please retry."},
        headers={"Retry-After": "3", "Cache-Control": "no-store"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(FileNotFoundError, file_not_found_handler)
    app.add_exception_handler(duckdb.OutOfMemoryException, duckdb_out_of_memory_handler)
