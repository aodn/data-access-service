"""Pure color utility functions for building colormap.json LUT entries."""

from typing import Any


def hex_to_rgba(hex_color: str) -> list[int]:
    """Convert a CSS hex color string to an [R, G, B, A] list."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    if len(h) == 6:
        return [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 255]
    if len(h) == 8:
        return [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), int(h[6:8], 16)]
    raise ValueError(f"invalid hex color: {hex_color!r}")


def parse_color(v: Any, label: str) -> list[int]:
    """Accept a hex string or [r, g, b, a] list and return a validated [R, G, B, A] list."""
    if isinstance(v, str):
        return hex_to_rgba(v)
    if isinstance(v, list | tuple):
        rgba = list(v)
        if len(rgba) != 4 or not all(
            isinstance(c, int) and 0 <= c <= 255 for c in rgba
        ):
            raise ValueError(f"{label} must be [r, g, b, a] with values 0–255")
        return rgba
    raise ValueError(f"{label} must be a hex string or [r, g, b, a] list")


def interpolate_colormap(stops: list[list[int]]) -> list[list[int]]:
    """Linearly interpolate N evenly-spaced RGBA stops to 256 entries."""
    import numpy as np

    arr = np.array(stops, dtype=float)
    x_stops = np.linspace(0, 1, len(stops))
    x_out = np.linspace(0, 1, 256)
    interpolated = np.stack(
        [np.interp(x_out, x_stops, arr[:, c]) for c in range(4)], axis=1
    )
    return np.clip(interpolated, 0, 255).round().astype(int).tolist()  # type: ignore[no-any-return]


def categorical_lut(categories: dict[int, list[int]]) -> list[list[int]]:
    """Map integer category values directly to a 256-entry RGBA LUT.

    Each value is its own slot — no rescaling — so distinct category codes can
    never collide onto the same slot the way a rescaled mapping could. Values
    outside 0-255 are skipped (rio-tiler's uint8 LUT can't represent them);
    categorical products use small non-negative codes in practice.
    """
    lut = [[0, 0, 0, 0] for _ in range(256)]
    for val, color in categories.items():
        if 0 <= val <= 255:
            lut[val] = color
    return lut
