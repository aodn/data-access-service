"""RGBA → PNG / WebP encoders."""

import io
from typing import Literal

import numpy as np
from PIL import Image

TILE_SIZE = 256

ImageFormat = Literal["png", "webp"]
AnimatedFormat = Literal["gif", "apng", "webp"]

_WEBP_QUALITY = 85
_WEBP_METHOD = 4


def encode_rgba(arr: np.ndarray, fmt: ImageFormat = "png") -> bytes:
    buf = io.BytesIO()
    img = Image.fromarray(arr, "RGBA")
    if fmt == "webp":
        img.save(buf, format="WEBP", quality=_WEBP_QUALITY, method=_WEBP_METHOD)
    else:
        img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


def empty_tile(fmt: ImageFormat = "png") -> bytes:
    return encode_rgba(np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8), fmt)


def media_type(fmt: ImageFormat) -> str:
    return "image/webp" if fmt == "webp" else "image/png"


def resize_rgba(arr: np.ndarray, width: int, height: int) -> np.ndarray:
    if arr.shape[0] == height and arr.shape[1] == width:
        return arr
    img = Image.fromarray(arr, "RGBA").resize((width, height), Image.Resampling.NEAREST)
    return np.asarray(img)


def animated_media_type(fmt: AnimatedFormat) -> str:
    if fmt == "gif":
        return "image/gif"
    if fmt == "webp":
        return "image/webp"
    return "image/apng"


def encode_rgba_animation(
    frames: list[np.ndarray], fmt: AnimatedFormat, duration_ms: int
) -> bytes:
    if not frames:
        raise ValueError("encode_rgba_animation requires at least one frame")
    images = [Image.fromarray(f, "RGBA") for f in frames]
    buf = io.BytesIO()
    extra = {
        "duration": duration_ms,
        "save_all": True,
        "append_images": images[1:],
        "loop": 0,
    }
    if fmt == "gif":
        images[0].save(buf, format="GIF", **extra)
    elif fmt == "webp":
        images[0].save(buf, format="WEBP", **extra)
    else:
        images[0].save(buf, format="PNG", **extra)
    return buf.getvalue()
