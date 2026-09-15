"""Colour a precomputed vector slice and encode PNG/WebP.

The parquet only stores ``value``. The LUT is applied here so colormap /
rescale can change without rewriting the file.
"""

from __future__ import annotations

import json
from typing import Literal

import numpy as np

from data_access_service.batch.tiler.generator import local_meta_path
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.tiler.utils.image import encode_rgba  # parquet-backed encoder

ImageFormat = Literal["png", "webp"]


def _default_ramp() -> np.ndarray:
    t = np.linspace(0.0, 1.0, 256)
    r = np.clip(1.5 * t - 0.2, 0, 1)
    g = np.clip(1.5 * t - 0.6, 0, 1)
    b = np.clip(1.2 - 1.4 * t, 0, 1)
    a = np.ones(256)
    return (np.stack([r, g, b, a], axis=1) * 255).astype(np.uint8)


def lut_from_name(name: str | None) -> np.ndarray:
    """Return a (256, 4) uint8 LUT. Unknown names fall back to a blue→red ramp."""
    key = (name or "ramp").lower()
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


def _load_meta(client: TilerDuckDBClient, uuid: str, variable: str | None) -> dict:
    path = local_meta_path(client._config.output_dir, uuid)
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    variables = payload.get("variables") or []
    if variable is not None:
        for frag in variables:
            if frag.get("variable") == variable:
                return frag
        raise FileNotFoundError(f"variable {variable!r} not in {path}")
    if not variables:
        raise FileNotFoundError(f"no variables in {path}")
    return variables[0]


def _paint(
    i: np.ndarray,
    j: np.ndarray,
    values: np.ndarray,
    n_i: int,
    n_j: int,
    vmin: float,
    vmax: float,
    lut: np.ndarray,
) -> np.ndarray:
    grid = np.full((n_i, n_j), np.nan, dtype=np.float32)
    grid[i, j] = values
    rgba = np.zeros((n_i, n_j, 4), dtype=np.uint8)
    finite = np.isfinite(grid)
    span = vmax - vmin
    if span <= 0:
        span = 1.0
    idx = np.zeros(grid.shape, dtype=np.uint8)
    idx[finite] = np.clip((grid[finite] - vmin) / span * 255.0, 0, 255).astype(np.uint8)
    rgba[finite] = lut[idx[finite]]
    return rgba


def render_time_slice(
    client: TilerDuckDBClient,
    uuid: str,
    timestamp: str,
    *,
    variable: str | None = None,
    colormap: str | None = "viridis",
    fmt: ImageFormat = "png",
    vmin: float | None = None,
    vmax: float | None = None,
    i_min: int | None = None,
    i_max: int | None = None,
    j_min: int | None = None,
    j_max: int | None = None,
) -> bytes:
    """Read a cell window, apply ``colormap``, return PNG or WebP bytes."""
    meta = _load_meta(client, uuid, variable)
    variable = variable or meta.get("variable")
    cells = client.read_cells(
        uuid,
        timestamp,
        i_min=i_min,
        i_max=i_max,
        j_min=j_min,
        j_max=j_max,
        variable=variable,
    )
    n_i = int(meta["n_i"])
    n_j = int(meta["n_j"])
    if i_min is None:
        i_min = 0
    if j_min is None:
        j_min = 0
    if i_max is None:
        i_max = n_i - 1
    if j_max is None:
        j_max = n_j - 1
    height = int(i_max) - int(i_min) + 1
    width = int(j_max) - int(j_min) + 1
    if cells.empty:
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        return encode_rgba(rgba, fmt)

    i = cells["i"].to_numpy() - int(i_min)
    j = cells["j"].to_numpy() - int(j_min)
    values = cells["value"].to_numpy(dtype=np.float32)
    lo = float(meta["vmin"] if vmin is None else vmin)
    hi = float(meta["vmax"] if vmax is None else vmax)
    rgba = _paint(i, j, values, height, width, lo, hi, lut_from_name(colormap))
    return encode_rgba(rgba, fmt)
