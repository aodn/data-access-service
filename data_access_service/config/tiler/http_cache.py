"""HTTP caching policy for the tiler endpoints — single source of truth.

IMMUTABLE_CACHE_HEADERS — 1 year, immutable, gated on ?cv=<CACHE_VERSION>
  (require_cache_version). The URL fully determines the response bytes.
    GET /{product_id}/{date}/{z}/{x}/{y}.png           raw data tile
    GET /{product_id}/{date}/manifest.json             data tile manifest
    GET /{product_id}/{date}/point                     point lookup
    GET /{product_id}/{date}/{z}/{x}/{y}.{ext}         visual tile
    GET /{product_id}/{date}/bbox.{ext}                visual bbox render
    GET /{product_id}/{from_date}/{to_date}/animation.{ext}  animation
    GET /colormaps/{name}/legend                       colormap legend

REVALIDATE_CACHE_HEADERS — 5 minutes, must-revalidate, ETag (etag_response).
  Response can change without the URL changing.
    GET /manifest    products availability
    GET /products    product list
    GET /colormaps   colormap list

CACHE_VERSION (?cv=) — how we bust the 1-year cache on purpose.
  A browser holding an "immutable, max-age=1yr" response will never ask the
  server for a fresher one. So to ship a fix, we don't update the response —
  we change the URL, which is a guaranteed cache miss.
    1. CACHE_VERSION = "cv1" is bumped by hand when rendered output actually
       changes (renderer code, product config, a colormap) — not on every deploy.
    2. Clients learn the current value from /manifest's `cache_version` field.
    3. Clients append it as `?cv=cv1` on every immutable-endpoint request.
    4. require_cache_version (a per-route dependency) 400s the request if
       `cv` is missing or doesn't match — this stops a client from ever
       caching an un-versioned URL that a future bump couldn't reach.
  /manifest itself skips this check: it's how clients find out what `cv`
  should be, so requiring it there would be a chicken-and-egg deadlock.

ETag — how REVALIDATE endpoints avoid resending a body that hasn't changed.
    1. compute_etag hashes a short fingerprint of "what would make this
       response different" (for /manifest: cv + query params + each
       product's dates; for /products and /colormaps: the payload itself).
    2. The response carries that hash in an `ETag` header.
    3. On the next request, the client sends back `If-None-Match: <etag>`.
    4. etag_response compares it: same hash → `304 Not Modified`, no body;
       different hash → `200` with the new body and new ETag.
  The server still does the same work either way (there's no server-side
  cache) — the saving is on the wire, not on the backend.
"""

import hashlib
from typing import Any

from fastapi import HTTPException, Query
from starlette.responses import JSONResponse, Response

CACHE_VERSION = "cv1"

IMMUTABLE_CACHE_HEADERS = {"Cache-Control": f"public, max-age={86400 * 365}, immutable"}
REVALIDATE_CACHE_HEADERS = {"Cache-Control": "public, max-age=300, must-revalidate"}


def require_cache_version(
    cv: str | None = Query(
        None,
        description=(
            "Cache-busting version tag. Must match the current value advertised by "
            "GET /manifest's `cache_version` field."
        ),
    ),
) -> None:
    """FastAPI dependency: 400s a request to an immutable endpoint with a stale/missing cv.

    Add as `Depends(require_cache_version)` on every route returning
    IMMUTABLE_CACHE_HEADERS. Deliberately not applied to REVALIDATE endpoints —
    see the module docstring for why /manifest can't be gated on `cv`.
    """
    if cv != CACHE_VERSION:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Missing or stale cache_version {cv!r}; expected {CACHE_VERSION!r}. "
                f"Fetch GET /manifest for the current value and retry with ?cv={CACHE_VERSION}"
            ),
        )


def compute_etag(fingerprint: str) -> str:
    digest = hashlib.sha1(fingerprint.encode(), usedforsecurity=False).hexdigest()[:16]
    return f'W/"{digest}"'


def etag_response(body: Any, etag: str, if_none_match: str | None) -> Response:
    """Build a REVALIDATE response, short-circuiting to 304 when the ETag matches."""
    headers = {**REVALIDATE_CACHE_HEADERS, "ETag": etag}
    if if_none_match == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(content=body, headers=headers)
