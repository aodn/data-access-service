"""Browser-safe coverage discovery for one portal collection.

Feeds ogcapi-java's coverage-tile catalog: everything it needs to publish OGC
tileset/TileMatrixSet/profile documents and to map OGC requests onto DAS tile
URLs — product identity, ordered variables, encoding, grid geometry per LOD,
the custom TileMatrixSet id, and the datetime ↔ date_key mapping.

Deliberately excluded: source paths, store URLs, credentials, or anything else
about where the data physically lives. This document crosses the trust boundary
(ogcapi-java republishes it to browsers), so keep it clean by construction —
there is a route test asserting no ``s3://``/``source_path`` ever appears.

Per-date decode ranges are NOT here either: they live in the existing per-date
``/data_tiles/{product}/{date}/manifest.json``, which the ogcapi-java profile
endpoint consumes per request.
"""

from typing import Any

from data_access_service.config.tiler.constants import CACHE_VERSION, LOD
from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
    parse_flag_values_and_meanings,
)
from data_access_service.tiler.services.product.grid_geometry import lod_grid_geometry
from data_access_service.tiler.services.product.product import Product, get_lod_grids
from data_access_service.tiler.services.product.registry import iter_products
from data_access_service.tiler.services.store.registry import get_store, store_registry
from data_access_service.tiler.utils.dates import ts_to_local_rfc3339
from data_access_service.tiler.utils.geo import dataset_bounds


def coverage_products(collection_id: str) -> list[Product]:
    """Coverage-enabled products associated with the given portal collection."""
    return [
        p
        for p in iter_products()
        if p.coverage is not None
        and p.coverage.enabled
        and p.portal is not None
        and p.portal.collection_id == collection_id
    ]


def _times(
    source_path: str, from_date: str | None, to_date: str | None
) -> list[dict[str, str]]:
    """datetime ↔ date_key pairs, one per available local date.

    Only the timestamp DAS actually selects for a date (the first source
    timestamp on that local date) is advertised, so discovery and tile
    retrieval cannot disagree (build spec §5).
    """
    index = store_registry.date_index(source_path)
    out = []
    for date_key in sorted(index):
        if from_date and date_key < from_date:
            continue
        if to_date and date_key > to_date:
            continue
        out.append(
            {"datetime": ts_to_local_rfc3339(index[date_key][0]), "dateKey": date_key}
        )
    return out


def _product_entry(
    product: Product, from_date: str | None, to_date: str | None
) -> dict[str, Any]:
    ds = get_store(product.source_path)  # also populates the date index
    lod_grids = get_lod_grids(product)
    lon_min, lon_max, lat_min, lat_max = dataset_bounds(ds)

    lods: dict[str, Any] = {}
    for lod, grid in sorted(lod_grids.items()):
        geom = lod_grid_geometry(ds, grid, product.chunk_px)
        lods[str(lod)] = {
            "grid": list(grid),
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

    variables = product.variables
    scalar = len(variables) == 1
    entry: dict[str, Any] = {
        "id": product.id,
        "title": product.title or product.id,
        "datasetKey": product.portal.dataset_key,
        "default": product.portal.default,
        "variables": list(variables),
        "encoding": "scalar-rgb24" if scalar else "vector-rg8",
        "maskChannel": "A" if scalar else "B",
        "tileMatrixSetId": product.coverage.tile_matrix_set_id,
        "bounds": {
            "lonMin": lon_min,
            "lonMax": lon_max,
            "latMin": lat_min,
            "latMax": lat_max,
        },
        "chunkPx": list(product.chunk_px),
        "storedPx": [
            product.chunk_px[0] + 2 * product.padding,
            product.chunk_px[1] + 2 * product.padding,
        ],
        "padding": product.padding,
        "lods": lods,
        "times": _times(product.source_path, from_date, to_date),
    }
    if scalar:
        attrs = ds[variables[0]].attrs
        if is_categorical_variable(attrs):
            values, labels = parse_flag_values_and_meanings(attrs)
            entry["flagValues"] = list(values)
            if labels is not None:
                entry["flagMeanings"] = list(labels)
    return entry


def build_coverage_discovery(
    collection_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any] | None:
    """The collection's coverage discovery document, or None when the collection
    has no coverage-enabled products (callers turn that into a 404)."""
    products = coverage_products(collection_id)
    if not products:
        return None
    return {
        "collectionId": collection_id,
        "cacheVersion": CACHE_VERSION,
        "products": [_product_entry(p, from_date, to_date) for p in products],
    }
