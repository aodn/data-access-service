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
  * ``GET /products`` is built from ``iter_products()`` (live ``Product``
    instances), not the raw JSON — so it reflects resolved defaults (e.g.
    ``ocean_masked`` via ``_OCEAN_MASKED_BY_DEFAULT``) rather than only what
    products.json literally spells out.
"""

import json
import logging
from pathlib import Path

from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH
from data_access_service.tiler.schemas.products import CoastalFillConfig, ProductConfig
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
    VisualTileConfig,
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


def load_products(dataset_uuid_map: dict[str, str] | None = None) -> None:
    """Read products.json from disk into PRODUCTS. Called once on startup.

    products.json is committed static config (config/tiler/products.json) — it should
    always be present on disk. A missing file means a broken deploy/package, not a
    legitimate empty state, so this raises rather than silently serving zero products.

    ``dataset_uuid_map`` (dataset filename -> metadata UUID, see
    ``API.get_dataset_uuid_map``) resolves each product's ``metadata_uuid`` from the
    runtime catalog rather than a hardcoded products.json field — that catalog is the
    actual source of truth, so a hand-copied value would just be another place for it
    to go stale. Pass ``None`` (the default) to skip resolution and leave
    ``metadata_uuid`` unset, e.g. in tests that don't care about it.

    Updates PRODUCTS in place without ever exposing an empty state to concurrent readers:
    additions/updates are applied first, then removals. A reader that races a reload sees
    either the previous set, the new set, or a transient with stale entries still
    present — never an empty dict.
    """
    if not _config_path.exists():
        raise FileNotFoundError(f"products.json not found at {_config_path}")
    entries: list[dict] = json.loads(_config_path.read_text())
    new = {entry["id"]: _from_dict(entry, dataset_uuid_map) for entry in entries}
    for product_id, product in new.items():
        PRODUCTS[product_id] = product
    for stale_id in [k for k in PRODUCTS if k not in new]:
        del PRODUCTS[stale_id]
    logger.info(f"Loaded {len(PRODUCTS)} products from {_config_path}")


def _coastal_fill(config: CoastalFillConfig | None) -> CoastalFill | None:
    return CoastalFill(max_dist_px=config.max_dist_px) if config else None


def _resolve_metadata_uuid(
    entry: dict, dataset_uuid_map: dict[str, str] | None
) -> str | None:
    """Look up ``entry``'s metadata UUID in the runtime catalog map by matching
    its ``source_path`` basename against a catalog dataset filename. Logs and
    returns None on a miss rather than failing startup — metadata_uuid is only
    used for GeoNetwork/STAC grouping, not for serving tiles, so a product
    should still load without it.
    """
    if dataset_uuid_map is None:
        return None
    dataset_name = entry["source_path"].rstrip("/").rsplit("/", 1)[-1]
    uuid = dataset_uuid_map.get(dataset_name)
    if uuid is None:
        logger.warning(
            "No metadata_uuid found in runtime catalog for product '%s' "
            "(dataset '%s') — loading it with metadata_uuid=None",
            entry["id"],
            dataset_name,
        )
    return uuid


def _from_dict(entry: dict, dataset_uuid_map: dict[str, str] | None = None) -> Product:
    """Validate one products.json entry against ProductConfig (extra="forbid"
    catches typos), after resolving the one default that depends on ``id``
    (ocean_masked) — every other default (chunk_px, padding) lives directly on
    ProductConfig/DataTileConfig and applies automatically when omitted.

    metadata_uuid is resolved from the runtime catalog (see
    _resolve_metadata_uuid), not read from the entry. It stays a declared field
    on ProductConfig only because that same model serializes GET /products
    output — extra="forbid" can't catch a stray "metadata_uuid" left in
    products.json, so that's rejected explicitly here instead.
    """
    if "metadata_uuid" in entry:
        raise ValueError(
            f"Product '{entry.get('id')}': metadata_uuid is resolved from the runtime "
            "catalog at load time and must not be set in products.json"
        )
    payload = dict(entry)
    if payload.get("ocean_masked") is None:
        payload["ocean_masked"] = entry["id"] in _OCEAN_MASKED_BY_DEFAULT
    parsed = ProductConfig(**payload)
    return Product(
        id=parsed.id,
        source_path=parsed.source_path,
        variable=parsed.variable,
        ocean_masked=parsed.ocean_masked,
        metadata_uuid=_resolve_metadata_uuid(entry, dataset_uuid_map),
        data_tile=DataTileConfig(
            chunk_px=parsed.data_tile.chunk_px,
            padding=parsed.data_tile.padding,
            coastal_fill=_coastal_fill(parsed.data_tile.coastal_fill),
        ),
        visual_tile=VisualTileConfig(
            coastal_fill=_coastal_fill(parsed.visual_tile.coastal_fill),
        ),
    )
