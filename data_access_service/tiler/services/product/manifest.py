"""The data-tile manifest: bounds, value ranges and LOD grids the client
needs to decode raw tiles."""

import math
from typing import Any

from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
    parse_flag_values_and_meanings,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.rendering.data_tiles import EMPTY_RANGE
from data_access_service.tiler.services.store.sparse_grid import SparseSlice
from data_access_service.tiler.utils.geo import json_safe_float


def _range(sparse: SparseSlice, var: str) -> list[float | None]:
    grid = sparse.grids[var]
    if math.isnan(grid.vmin) or math.isnan(grid.vmax):
        return list(EMPTY_RANGE)
    return [json_safe_float(grid.vmin), json_safe_float(grid.vmax)]


def render_manifest(product: Product, sparse: SparseSlice) -> dict[str, Any]:
    lon_min, lon_max, lat_min, lat_max = sparse.bounds()
    bounds = {
        "lonMin": lon_min,
        "lonMax": lon_max,
        "latMin": lat_min,
        "latMax": lat_max,
    }
    data_tile = product.data_tile
    lod_meta = {
        str(lod): {
            "grid": list(data_tile.lod_grids[lod]),
            "chunkPx": list(data_tile.chunk_px),
            "storedPx": [
                data_tile.chunk_px[0] + 2 * data_tile.padding,
                data_tile.chunk_px[1] + 2 * data_tile.padding,
            ],
            "padding": data_tile.padding,
        }
        for lod in data_tile.lod_grids
    }

    if isinstance(product.variable, list):
        u_var, v_var = product.variable
        return {
            "bounds": bounds,
            "uRange": _range(sparse, u_var),
            "vRange": _range(sparse, v_var),
            "lods": lod_meta,
        }
    manifest: dict[str, Any] = {
        "bounds": bounds,
        "valueRange": _range(sparse, product.variable),
        "lods": lod_meta,
    }
    # Categorical variables also get their codes, and labels when available.
    attrs = sparse.attrs[product.variable]
    if is_categorical_variable(attrs):
        values, labels = parse_flag_values_and_meanings(attrs)
        manifest["flagValues"] = list(values)
        if labels is not None:
            manifest["flagMeanings"] = list(labels)
    return manifest
