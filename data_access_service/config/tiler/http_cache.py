"""HTTP caching policy for the tiler endpoints — single source of truth.

IMMUTABLE_CACHE_HEADERS — 1 year at the CDN, no browser caching.
    GET /{product_id}/{date}/{z}/{x}/{y}.png           raw data tile
    GET /{product_id}/{date}/manifest.json             data tile manifest
    GET /{product_id}/{date}/point                     point lookup
    GET /{product_id}/{date}/{z}/{x}/{y}.{ext}         visual tile
    GET /{product_id}/{date}/bbox.{ext}                visual bbox render
    GET /{product_id}/{from_date}/{to_date}/animation.{ext}  animation
    GET /colormaps/{name}/legend                       colormap legend

REVALIDATE_CACHE_HEADERS — 5 minutes, must-revalidate, no ETag.
  Response can change without the URL changing.
    GET /manifest    products availability
    GET /products    product list
    GET /colormaps   colormap list

CloudFront settings required to honour the above:
    1. Cache key includes all query strings.
    2. Cache policy TTL bounds: MinTTL=0, MaxTTL=31536000 (1 year) — wide
       enough that CloudFront never clamps below what Cache-Control says.
    3. CloudFront must respect `s-maxage` over `max-age` (the default
       CachingOptimized-style behaviour) so immutable responses get a
       year at the edge despite telling browsers `max-age=0`.
"""

IMMUTABLE_CACHE_HEADERS = {
    "Cache-Control": f"public, s-maxage={86400 * 365}, max-age=0, must-revalidate"
}
REVALIDATE_CACHE_HEADERS = {"Cache-Control": "public, max-age=300, must-revalidate"}
