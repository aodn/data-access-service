"""Geo helpers for the renderers."""

import math


def json_safe_float(v) -> float | None:
    """float(v), or None if NaN or infinite."""
    f = float(v)
    return None if math.isnan(f) or math.isinf(f) else f
