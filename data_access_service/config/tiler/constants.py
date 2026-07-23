from dataclasses import dataclass

# Applied by the store registry to normalise source coordinate names so the rest
# of the pipeline can assume `time` / `lat` / `lon` regardless of how a product
# names its dimensions upstream.
COORD_NAMES = {"TIME": "time", "LATITUDE": "lat", "LONGITUDE": "lon"}


@dataclass(frozen=True)
class LODConfig:
    """Server-shader contract for the data-tile LOD pyramid.

    Bundled here (rather than passed at runtime or read from env) because these
    values are baked into the WebGL shader on the frontend — changing one without
    redeploying the frontend silently corrupts the rendering.
    """

    # Cap on LOD levels per product. The frontend packs all LODs into a single WebGL
    # texture atlas hard-capped at 4096×4096 (~64 MB VRAM per atlas) regardless of
    # gl.MAX_TEXTURE_SIZE. Going above 4 doesn't break rendering — the atlas falls
    # back to LRU eviction — but causes visible tile re-upload churn as the user
    # pans/zooms. 4 is the value tuned to fit comfortably under the cap.
    max_lods: int = 4
    # Minimum (cols, rows) for the coarsest level; levels below this are dropped.
    # But if a product only has fewer than this, it will still be served below this
    # threshold.
    min_coarsest: tuple[int, int] = (2, 2)


LOD = LODConfig()


@dataclass(frozen=True)
class TileConfig:
    """Per-product tile geometry defaults — also part of the server↔shader contract.

    ``chunk_px`` is the visible tile size; ``padding`` is the extra ring of edge
    pixels included on each side so the shader can sample a bilinear filter
    without seams between tiles. Both are overridable per product in products.json, but the defaults here are.
    """

    chunk_px: tuple[int, int] = (240, 192)
    padding: int = 1


TILE = TileConfig()
