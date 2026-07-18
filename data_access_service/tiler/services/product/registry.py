"""In-memory ``Product`` registry, loaded from ``products.json`` at startup.

Single front door for everything product-related at runtime:

  * The ``PRODUCTS`` dict is the canonical registered-product state. Internal
    consumers (test fixtures, the prewarm race-guard) still touch it directly
    where the dict's identity matters; production callers should go through
    the facades (``get_product``, ``iter_products``, ``iter_product_items``).
  * ``load_products`` reads the on-disk ``products.json`` into the in-memory
    dict. Products are static config (``config/products.json``) — add or
    remove one by editing the file and redeploying.
  * ``id`` convention: ``{zarr_name}:{variable}``, e.g.
    ``satellite_austemp_heatwave_8day:sst_mosaic`` — the colon separates the
    Zarr store name (from ``source_path``) from the variable it exposes,
    since both may themselves contain underscores. Multi-variable products
    join variables with ``+`` in ``variable`` array order, e.g.
    ``model_sea_level_anomaly_gridded_realtime:ucur+vcur``. This is a
    readability convention only — ``id`` is never parsed, just used as an
    opaque lookup key — so it isn't enforced in code.
  * ``list_products`` returns the raw JSON entries (used by ``GET /products``);
    this is intentionally different from ``iter_products()`` which returns live
    ``Product`` instances.
"""

import json
import logging
import uuid
from collections import defaultdict
from pathlib import Path

from data_access_service.config.tiler.constants import TILE
from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    CoverageConfig,
    PortalAssociation,
    Product,
)

logger = logging.getLogger(__name__)

_config_path = Path(PRODUCTS_CONFIG_PATH)

# Products that are ocean-masked unless products.json says otherwise. The committed
# ocean mask is built from this store's grid, so masking is the safe default for it
# and shouldn't depend on the config flag being remembered. An explicit
# "ocean_masked": false in products.json still wins.
_OCEAN_MASKED_BY_DEFAULT = frozenset(
    {
        "model_sea_level_anomaly_gridded_realtime:ucur+vcur",
    }
)

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


def load_products() -> None:
    """Read products.json from disk into PRODUCTS. Called once on startup.

    Updates PRODUCTS in place without ever exposing an empty state to concurrent readers:
    additions/updates are applied first, then removals. A reader that races a reload sees
    either the previous set, the new set, or a transient with stale entries still
    present — never an empty dict.
    """
    if not _config_path.exists():
        logger.warning("No products.json found — starting with empty product list")
        return
    entries: list[dict] = json.loads(_config_path.read_text())
    new = {entry["id"]: _from_dict(entry) for entry in entries}
    validate_coverage_config(new.values())
    for product_id, product in new.items():
        PRODUCTS[product_id] = product
    for stale_id in [k for k in PRODUCTS if k not in new]:
        del PRODUCTS[stale_id]
    logger.info(f"Loaded {len(PRODUCTS)} products from {_config_path}")


def list_products() -> list[dict]:
    """Return the raw JSON entries from products.json. Used by ``GET /products``.

    Distinct from ``iter_products()`` which returns live ``Product`` instances.
    This returns whatever the config file says, including fields that may not
    be on the Product dataclass.
    """
    if not _config_path.exists():
        return []
    return json.loads(_config_path.read_text())


def _from_dict(entry: dict) -> Product:
    chunk_px = entry.get("chunk_px", list(TILE.chunk_px))
    coastal_fill = entry.get("coastal_fill")
    portal = entry.get("portal")
    coverage = entry.get("coverage")
    return Product(
        id=entry["id"],
        source_path=entry["source_path"],
        variable=entry["variable"],
        chunk_px=tuple(chunk_px),  # type: ignore[arg-type]
        padding=entry.get("padding", TILE.padding),
        coastal_fill=CoastalFill(**coastal_fill) if coastal_fill else None,
        ocean_masked=entry.get("ocean_masked", entry["id"] in _OCEAN_MASKED_BY_DEFAULT),
        title=entry.get("title"),
        portal=PortalAssociation(**portal) if portal else None,
        coverage=CoverageConfig(**coverage) if coverage else None,
    )


def validate_coverage_config(products) -> None:
    """Startup validation of the portal/coverage association across all products.

    Fails product loading loudly (ValueError) rather than serving a coverage
    surface that would misroute requests. Rules (build spec §5):
      * coverage.enabled requires a portal association and a tile_matrix_set_id;
      * portal.collection_id must be a valid UUID;
      * coverage products have one or two variables (the renderer's limit);
      * within a collection: exactly one default coverage product, and each
        variable set resolves to exactly one product;
      * a shared tile_matrix_set_id means identical grid geometry — enforced
        strictly as same source store + chunk_px + padding (grid geometry is a
        pure function of those; different stores may not share an id even if
        their grids coincide today, because they can drift apart silently).
    """
    by_collection: dict[str, list[Product]] = defaultdict(list)
    by_tms: dict[str, list[Product]] = defaultdict(list)

    for p in products:
        if p.coverage is None or not p.coverage.enabled:
            continue
        if p.portal is None:
            raise ValueError(f"Product '{p.id}': coverage.enabled requires portal")
        if not p.coverage.tile_matrix_set_id:
            raise ValueError(
                f"Product '{p.id}': coverage.enabled requires coverage.tile_matrix_set_id"
            )
        try:
            uuid.UUID(p.portal.collection_id)
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(
                f"Product '{p.id}': portal.collection_id is not a valid UUID: "
                f"{p.portal.collection_id!r}"
            ) from e
        if len(p.variables) > 2:
            raise ValueError(
                f"Product '{p.id}': coverage products support 1 or 2 variables, "
                f"got {len(p.variables)}"
            )
        by_collection[p.portal.collection_id].append(p)
        by_tms[p.coverage.tile_matrix_set_id].append(p)

    for collection_id, members in by_collection.items():
        defaults = [p.id for p in members if p.portal.default]
        if len(defaults) != 1:
            raise ValueError(
                f"Collection {collection_id}: exactly one default coverage product "
                f"required, got {defaults or 'none'}"
            )
        seen_variable_sets: dict[frozenset, str] = {}
        for p in members:
            key = frozenset(p.variables)
            if key in seen_variable_sets:
                raise ValueError(
                    f"Collection {collection_id}: variable set {sorted(key)} maps to "
                    f"both '{seen_variable_sets[key]}' and '{p.id}'"
                )
            seen_variable_sets[key] = p.id

    for tms_id, members in by_tms.items():
        geometries = {(p.source_path, tuple(p.chunk_px), p.padding) for p in members}
        if len(geometries) > 1:
            raise ValueError(
                f"TileMatrixSet '{tms_id}' is shared by products with different grid "
                f"geometry: {sorted(p.id for p in members)}"
            )
