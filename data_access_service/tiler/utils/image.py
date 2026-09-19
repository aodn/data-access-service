"""RGBA image encoders.

Data tiles must be PNG with ``optimize=False``: the client decodes the bytes
as values. Visual tiles may also be lossy WebP, except for categorical
colormaps.
"""

import io
from typing import Literal

import numpy as np
from PIL import Image

TILE_SIZE = 256

ImageFormat = Literal["png", "webp"]
AnimatedFormat = Literal["gif", "apng", "webp"]

_WEBP_QUALITY = 85
_WEBP_METHOD = 4  # 0 fast .. 6 best


def encode_rgba(arr: np.ndarray, fmt: ImageFormat = "png") -> bytes:
    """(H, W, 4) uint8 RGBA -> PNG or WebP bytes."""
    buf = io.BytesIO()
    img = Image.fromarray(arr, "RGBA")
    if fmt == "webp":
        img.save(buf, format="WEBP", quality=_WEBP_QUALITY, method=_WEBP_METHOD)
    else:
        img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


def _build_empty_tile(fmt: ImageFormat) -> bytes:
    return encode_rgba(np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8), fmt)


_EMPTY_TILES: dict[ImageFormat, bytes] = {
    "png": _build_empty_tile("png"),
    "webp": _build_empty_tile("webp"),
}


def empty_tile(fmt: ImageFormat = "png") -> bytes:
    return _EMPTY_TILES[fmt]


def media_type(fmt: ImageFormat) -> str:
    return "image/webp" if fmt == "webp" else "image/png"


def animated_media_type(fmt: AnimatedFormat) -> str:
    if fmt == "gif":
        return "image/gif"
    if fmt == "webp":
        return "image/webp"
    return "image/apng"


def encode_rgba_animation(
    frames: list[np.ndarray], fmt: AnimatedFormat, duration_ms: int
) -> bytes:
    """RGBA frames -> an animated GIF, WebP or APNG. GIF is limited to 256
    colours."""
    if not frames:
        raise ValueError("encode_rgba_animation requires at least one frame")

    images = [Image.fromarray(f, "RGBA") for f in frames]
    head = images[0]
    tail = images[1:]
    buf = io.BytesIO()

    if fmt == "gif":
        head.save(
            buf,
            format="GIF",
            save_all=True,
            append_images=tail,
            duration=duration_ms,
            loop=0,
            disposal=2,
        )
    elif fmt == "webp":
        head.save(
            buf,
            format="WEBP",
            save_all=True,
            append_images=tail,
            duration=duration_ms,
            loop=0,
            quality=_WEBP_QUALITY,
            method=_WEBP_METHOD,
        )
    else:  # apng
        head.save(
            buf,
            format="PNG",
            save_all=True,
            append_images=tail,
            duration=duration_ms,
            loop=0,
        )
    return buf.getvalue()
