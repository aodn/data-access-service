from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
from fastapi import HTTPException

from data_access_service.config.tiler.paths import COLORMAPS_CONFIG_PATH

logger = logging.getLogger(__name__)

_custom: dict[str, np.ndarray] = {}


def _default_ramp() -> np.ndarray:
    t = np.linspace(0.0, 1.0, 256)
    r = np.clip(1.5 * t - 0.2, 0, 1)
    g = np.clip(1.5 * t - 0.6, 0, 1)
    b = np.clip(1.2 - 1.4 * t, 0, 1)
    a = np.ones(256)
    return (np.stack([r, g, b, a], axis=1) * 255).astype(np.uint8)


def lut_from_name(name: str | None) -> np.ndarray:
    key = (name or "viridis").lower()
    if key in _custom:
        return _custom[key]
    if key in {"gray", "grey", "grayscale"}:
        x = np.arange(256, dtype=np.uint8)
        return np.stack([x, x, x, np.full(256, 255, dtype=np.uint8)], axis=1)
    if key in {"ramp", "default"}:
        return _default_ramp()
    try:
        from matplotlib import colormaps

        cmap = colormaps[key]
        return (np.asarray(cmap(np.linspace(0.0, 1.0, 256))) * 255).astype(np.uint8)
    except (KeyError, ValueError, OSError):
        return _default_ramp()


def load_colormaps() -> None:
    _custom.clear()
    path = Path(COLORMAPS_CONFIG_PATH)
    if not path.exists():
        logger.warning("No colormaps.json at %s", path)
        return
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        logger.exception("Failed to parse %s", path)
        return
    for name, spec in data.items():
        if not isinstance(spec, dict):
            continue
        entries = spec.get("entries")
        if isinstance(entries, list) and len(entries) == 256:
            _custom[name] = np.asarray(entries, dtype=np.uint8)
    logger.info("Loaded %s custom colormaps", len(_custom))


def list_colormaps() -> dict:
    matplotlib_names: list[str] = []
    try:
        from matplotlib import colormaps

        matplotlib_names = sorted(colormaps)
    except Exception:
        pass
    return {
        "custom": [{"name": n, "mode": "ramp"} for n in sorted(_custom)],
        "rio_tiler": [],
        "matplotlib": matplotlib_names,
    }


def resolve_colormap_or_error(name: str, status_code: int = 400) -> None:
    lut = lut_from_name(name)
    if name.lower() not in {"viridis", "gray", "grey", "grayscale", "ramp", "default"}:
        if name not in _custom:
            try:
                from matplotlib import colormaps

                if name not in colormaps and name.lower() not in colormaps:
                    raise HTTPException(
                        status_code=status_code, detail=f"Unknown colormap: {name}"
                    )
            except HTTPException:
                raise
            except Exception:
                pass
    _ = lut
