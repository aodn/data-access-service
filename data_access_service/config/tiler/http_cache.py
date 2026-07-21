"""HTTP caching policy for the tiler endpoints — single source of truth.

IMMUTABLE_CACHE_HEADERS — 1 year at the CDN, no browser caching.
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

ETag — how REVALIDATE endpoints avoid resending a body that hasn't changed.
    1. compute_etag hashes a short fingerprint of "what would make this
       response different" (for /manifest: query params + each product's
       dates; for /products and /colormaps: the payload itself).
    2. The response carries that hash in an `ETag` header.
    3. On the next request, the client sends back `If-None-Match: <etag>`.
    4. etag_response compares it: same hash → `304 Not Modified`, no body;
       different hash → `200` with the new body and new ETag.
  The server still does the same work either way (there's no server-side
  cache) — the saving is on the wire, not on the backend.

CloudFront settings required to honour the above:
    1. Cache key includes all query strings.
    2. Cache policy TTL bounds: MinTTL=0, MaxTTL=31536000 (1 year) — wide
       enough that CloudFront never clamps below what Cache-Control says.
    3. CloudFront must respect `s-maxage` over `max-age` (the default
       CachingOptimized-style behaviour) so immutable responses get a
       year at the edge despite telling browsers `max-age=0`.
"""

import hashlib
from typing import Any

from starlette.responses import JSONResponse, Response

IMMUTABLE_CACHE_HEADERS = {
    "Cache-Control": f"public, s-maxage={86400 * 365}, max-age=0, must-revalidate"
}
REVALIDATE_CACHE_HEADERS = {"Cache-Control": "public, max-age=300, must-revalidate"}


def compute_etag(fingerprint: str) -> str:
    digest = hashlib.sha1(fingerprint.encode(), usedforsecurity=False).hexdigest()[:16]
    return f'W/"{digest}"'


def etag_response(body: Any, etag: str, if_none_match: str | None) -> Response:
    """Build a REVALIDATE response, short-circuiting to 304 when the ETag matches."""
    headers = {**REVALIDATE_CACHE_HEADERS, "ETag": etag}
    if if_none_match == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(content=body, headers=headers)
