"""The data-tile manifest: bounds, value ranges and LOD grids the client
needs to decode raw tiles."""

from typing import Any

import xarray as xr

from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
    parse_flag_values_and_meanings,
)
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.utils.geo import json_safe_float


def render_manifest(product: Product, ds: xr.Dataset) -> dict[str, Any]:
    lon_min_g = float(ds.lon.min())
    lon_max_g = float(ds.lon.max())
    lat_min_g = float(ds.lat.min())
    lat_max_g = float(ds.lat.max())

    bounds = {
        "lonMin": lon_min_g,
        "lonMax": lon_max_g,
        "latMin": lat_min_g,
        "latMax": lat_max_g,
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
            "uRange": [
                json_safe_float(ds[u_var].min(skipna=True).values),
                json_safe_float(ds[u_var].max(skipna=True).values),
            ],
            "vRange": [
                json_safe_float(ds[v_var].min(skipna=True).values),
                json_safe_float(ds[v_var].max(skipna=True).values),
            ],
            "lods": lod_meta,
        }
    manifest: dict[str, Any] = {
        "bounds": bounds,
        "valueRange": [
            json_safe_float(ds[product.variable].min(skipna=True).values),
            json_safe_float(ds[product.variable].max(skipna=True).values),
        ],
        "lods": lod_meta,
    }
    # Categorical variables also get their codes, and labels when available.
    attrs = ds[product.variable].attrs
    if is_categorical_variable(attrs):
        values, labels = parse_flag_values_and_meanings(attrs)
        manifest["flagValues"] = list(values)
        if labels is not None:
            manifest["flagMeanings"] = list(labels)
    return manifest
