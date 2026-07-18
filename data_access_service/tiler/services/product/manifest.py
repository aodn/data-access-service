"""Build the JSON manifest returned by ``/data_tiles/{product}/{date}/manifest.json``.

Pure product introspection: takes a product and a slice dataset, returns the
bounds + per-variable value range + per-LOD grid metadata the WebGL shader
needs to decode raw data tiles. No rendering, no caching — lives next to the
product domain rather than the rendering pipeline because the output is a
description of the product's data shape on this date, not a pixel artifact.
"""

from typing import Any

import xarray as xr

from data_access_service.config.tiler.constants import LOD
from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
    parse_flag_values_and_meanings,
)
from data_access_service.tiler.services.product.grid_geometry import lod_grid_geometry
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

    def _lod_meta(lod: int) -> dict[str, Any]:
        # Square-cell NW-anchored geometry (see product/grid_geometry.py): each LOD
        # grid has one cellSize and its own geographic footprint (gridBounds, cell
        # EDGES) that can extend past the data on the east/south — those pixels are
        # rendered mask-invalid. Clients must georeference each LOD by gridBounds,
        # not by the data `bounds` above.
        geom = lod_grid_geometry(ds, product.lod_grids[lod], product.chunk_px)
        return {
            "grid": list(product.lod_grids[lod]),
            "chunkPx": list(product.chunk_px),
            "storedPx": [
                product.chunk_px[0] + 2 * product.padding,
                product.chunk_px[1] + 2 * product.padding,
            ],
            "padding": product.padding,
            "cellSize": geom.cell_size,
            "gridBounds": {
                "lonMin": geom.west,
                "lonMax": geom.east,
                "latMin": geom.south,
                "latMax": geom.north,
            },
            **(
                {"zoomThreshold": LOD.zoom_thresholds[lod]}
                if lod in LOD.zoom_thresholds
                else {}
            ),
        }

    lod_meta = {str(lod): _lod_meta(lod) for lod in product.lod_grids}

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
    # Categorical (CF flag_values) variable: surface the discrete codes and their
    # labels so the client can decode and label raw values without a second request.
    # parse_flag_values_and_meanings drops flagMeanings when absent or misaligned
    # with flagValues, so the key is simply omitted in that case.
    attrs = ds[product.variable].attrs
    if is_categorical_variable(attrs):
        values, labels = parse_flag_values_and_meanings(attrs)
        manifest["flagValues"] = list(values)
        if labels is not None:
            manifest["flagMeanings"] = list(labels)
    return manifest
