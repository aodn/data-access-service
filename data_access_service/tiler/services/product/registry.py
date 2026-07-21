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
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH
from data_access_service.tiler.services.product.product import CoastalFill, Product

logger = logging.getLogger(__name__)


class _CoastalFillEntry(BaseModel):
    max_dist_px: int


class _ProductEntry(BaseModel):
    """The allowlisted shape of one products.json entry."""

    model_config = ConfigDict(extra="forbid")

    id: str
    source_path: str
    variable: str | list[str]
    chunk_px: tuple[int, int] | None = None
    padding: int | None = None
    coastal_fill: _CoastalFillEntry | None = None
    zoom_thresholds: dict[int, int] | None = None
    ocean_masked: bool | None = None


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
    parsed = _ProductEntry(**entry)
    kwargs: dict = {}
    if parsed.chunk_px is not None:
        kwargs["chunk_px"] = parsed.chunk_px
    if parsed.padding is not None:
        kwargs["padding"] = parsed.padding
    if parsed.zoom_thresholds is not None:
        kwargs["zoom_thresholds"] = parsed.zoom_thresholds
    return Product(
        id=parsed.id,
        source_path=parsed.source_path,
        variable=parsed.variable,
        coastal_fill=(
            CoastalFill(max_dist_px=parsed.coastal_fill.max_dist_px)
            if parsed.coastal_fill
            else None
        ),
        ocean_masked=(
            parsed.ocean_masked
            if parsed.ocean_masked is not None
            else parsed.id in _OCEAN_MASKED_BY_DEFAULT
        ),
        **kwargs,
    )
