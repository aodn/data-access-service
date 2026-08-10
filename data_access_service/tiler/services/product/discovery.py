"""Discovers tiler products from the zarr metadata API already fetched at
startup, instead of hardcoding source_path/variable/metadata_uuid by hand.

For every zarr dataset API knows about, each entry in gridded_variables.json
(a variable name, or a list of variable names that must ALL be present) that
matches becomes one Product. products.json (see registry.py) then applies
optional per-product overrides on top by matching `id`.
"""

import json
import logging
from pathlib import Path

from data_access_service.config.tiler.paths import GRIDDED_VARIABLES_CONFIG_PATH
from data_access_service.core.api import API
from data_access_service.tiler.services.product.product import Product

logger = logging.getLogger(__name__)

_config_path = Path(GRIDDED_VARIABLES_CONFIG_PATH)

GriddedVariableSpec = str | list[str]


def _load_gridded_variable_specs() -> list[GriddedVariableSpec]:
    """Read gridded_variables.json: a flat list of variable names, where an
    entry can itself be a list of names that must ALL be present in a dataset
    to form one multi-variable product.

    In practice the only multi-variable group is ["UCUR", "VCUR"] — a
    current/wind/wave vector pair that only makes sense rendered together.
    Multi-variable products are a /data_tiles-only concept: /visual_tiles
    renders a single scalar variable through a colormap and rejects anything
    else via single_variable_or_400 (core/tiler_routes/shared.py), so a
    group entry here implicitly opts that product out of /visual_tiles.
    """
    if not _config_path.exists():
        raise FileNotFoundError(f"gridded_variables.json not found at {_config_path}")
    return json.loads(_config_path.read_text())


# It is sate to use the zarr stem and variable names to form a product id,
# because all the zarr dataset live in the same s3 bucket, it is impossible that zarrs share the same names.
def _product_id(zarr_stem: str, names: list[str]) -> str:
    return f"{zarr_stem}:{'+'.join(n.lower() for n in names)}"


def _products_for_dataset(
    uuid: str,
    dname: str,
    source_path: str,
    variables: frozenset,
    specs: list[GriddedVariableSpec],
) -> list[Product]:
    """Pure matcher: which of `specs` are fully present in `variables`.

    No API dependency, so this is the part exercised directly by unit tests —
    discover_products is just this plus the API glue.
    """
    zarr_stem = dname.removesuffix(".zarr")
    products = []
    for spec in specs:
        names = spec if isinstance(spec, list) else [spec]
        if all(name in variables for name in names):
            products.append(
                Product(
                    id=_product_id(zarr_stem, names),
                    source_path=source_path,
                    variable=names if len(names) > 1 else names[0],
                    metadata_uuid=uuid,
                )
            )
    return products


def discover_products(api: API) -> list[Product]:
    """Build the full discovered product list from every zarr dataset API
    indexed at init. Discovered products carry defaults (ocean_masked=False,
    default data_tile/visual_tile) — registry.load_products applies real
    defaults/overrides from products.json on top.
    """
    specs = _load_gridded_variable_specs()
    discovered = [
        product
        for uuid, dname, source_path, variables in api.iter_zarr_dataset_variables()
        for product in _products_for_dataset(uuid, dname, source_path, variables, specs)
    ]
    logger.info(f"Discovered {len(discovered)} products from {_config_path.name}")
    return discovered
