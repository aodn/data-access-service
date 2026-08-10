"""In-memory ``Product`` registry, populated at startup from discovered zarr
products plus optional overrides from ``products.json``.

Single front door for everything product-related at runtime:

  * The ``PRODUCTS`` dict is the canonical registered-product state. Internal
    consumers (test fixtures, the prewarm race-guard) still touch it directly
    where the dict's identity matters; production callers should go through
    the facades (``get_product``, ``iter_products``, ``iter_product_items``).
  * ``load_products`` takes the discovered product list (see
    ``services/product/discovery.py::discover_products``, called once at
    startup from the zarr catalog ``API`` already fetched) and applies
    ``products.json`` on top as per-``id`` overrides — ``ocean_masked``,
    ``data_tile``, ``visual_tile`` only. ``id``/``source_path``/``variable``/
    ``metadata_uuid`` come from discovery, not products.json: a discovered
    product needs a products.json entry only when it deviates from defaults.
  * ``id`` convention: ``{zarr_name}:{variable}``, e.g.
    ``satellite_austemp_heatwave_8day:sst_mosaic`` — the colon separates the
    Zarr store name (from ``source_path``) from the variable it exposes,
    since both may themselves contain underscores. Multi-variable products
    join variables with ``+`` in ``variable`` array order, e.g.
    ``model_sea_level_anomaly_gridded_realtime:ucur+vcur``. This convention
    is now load-bearing: it's exactly what ``discovery._product_id`` produces
    and what a products.json override's ``id`` must match to apply.
  * ``GET /products`` is built from ``iter_products()`` (live ``Product``
    instances), not the raw JSON — so it reflects resolved defaults (e.g.
    ``ocean_masked`` defaulting to False) rather than only what products.json
    literally spells out.
"""

import dataclasses
import json
import logging
from pathlib import Path

from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH
from data_access_service.tiler.schemas.products import (
    CoastalFillConfig,
    ProductOverride,
)
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
    VisualTileConfig,
)

logger = logging.getLogger(__name__)


_config_path = Path(PRODUCTS_CONFIG_PATH)

# Canonical registered-product state. Exposed (rather than wrapped behind a
# class) because the dict identity is load-bearing for test fixtures and for
# the prewarm race-guard ``PRODUCTS.get(p.id) is not p`` check — both rely on
# the same Python object being mutated in place.
PRODUCTS: dict[str, Product] = {}


def get_product(product_id: str) -> Product | None:
    """Return the registered Product for ``product_id``, or None if not registered."""
    return PRODUCTS.get(product_id)


def iter_products() -> list[Product]:
    """Snapshot of every registered Product.

    Returns a list (not a view) so a concurrent reload can't raise
    ``RuntimeError: dictionary changed size during iteration`` in the caller's loop.
    """
    return list(PRODUCTS.values())


def iter_product_items() -> list[tuple[str, Product]]:
    """Snapshot of every (product_id, Product) pair. Snapshot rationale: see iter_products."""
    return list(PRODUCTS.items())


def load_products(discovered: list[Product]) -> None:
    """Merge ``discovered`` (see discovery.discover_products) with
    products.json overrides into PRODUCTS. Called once on startup.

    products.json is committed static config (config/tiler/products.json) — it should
    always be present on disk (an empty ``[]`` is fine — it just means no product needs
    a non-default override). A missing file means a broken deploy/package, not a
    legitimate empty state, so this raises rather than silently serving zero overrides.

    An override entry whose ``id`` matches no discovered product (e.g. AODN renamed or
    removed a variable upstream) is logged and skipped rather than failing startup —
    the rest of the product set still loads.

    Updates PRODUCTS in place without ever exposing an empty state to concurrent readers:
    additions/updates are applied first, then removals. A reader that races a reload sees
    either the previous set, the new set, or a transient with stale entries still
    present — never an empty dict.
    """
    overrides = _load_overrides()
    new = {
        product.id: _apply_override(product, overrides.pop(product.id, None))
        for product in discovered
    }
    for stale_id in overrides:
        logger.warning(
            f"products.json override for '{stale_id}' matches no discovered "
            "product — ignoring"
        )
    for product_id, product in new.items():
        PRODUCTS[product_id] = product
    for stale_id in [k for k in PRODUCTS if k not in new]:
        del PRODUCTS[stale_id]
    logger.info(f"Loaded {len(PRODUCTS)} products ({len(discovered)} discovered)")


def _load_overrides() -> dict[str, ProductOverride]:
    if not _config_path.exists():
        raise FileNotFoundError(f"products.json not found at {_config_path}")
    entries: list[dict] = json.loads(_config_path.read_text())
    return {entry["id"]: ProductOverride(**entry) for entry in entries}


def _coastal_fill(config: CoastalFillConfig | None) -> CoastalFill | None:
    return CoastalFill(max_dist_px=config.max_dist_px) if config else None


def _apply_override(product: Product, override: ProductOverride | None) -> Product:
    """Resolve a discovered Product's config: an explicit products.json
    override wins, otherwise plain ocean_masked=False and
    DataTileConfig/VisualTileConfig defaults.
    """
    ocean_masked = False
    data_tile = DataTileConfig()
    visual_tile = VisualTileConfig()
    if override is not None:
        if override.ocean_masked is not None:
            ocean_masked = override.ocean_masked
        data_tile = DataTileConfig(
            chunk_px=override.data_tile.chunk_px,
            padding=override.data_tile.padding,
            coastal_fill=_coastal_fill(override.data_tile.coastal_fill),
        )
        visual_tile = VisualTileConfig(
            coastal_fill=_coastal_fill(override.visual_tile.coastal_fill),
        )
    return dataclasses.replace(
        product,
        ocean_masked=ocean_masked,
        data_tile=data_tile,
        visual_tile=visual_tile,
    )
