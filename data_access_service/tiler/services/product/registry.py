"""In-memory ``Product`` registry, published once at startup by tiler warmup.

Products arrive already derived ([[discovery]]) but not yet store-verified —
every discovered candidate is published unconditionally. Whether a product's
backing store actually opened is tracked separately by
``store.registry.is_store_available`` and enforced per-request, not by
registry membership.
"""

import logging

from data_access_service.tiler.services.product.product import Product

logger = logging.getLogger(__name__)

# Dict identity is load-bearing: test fixtures and the prewarm race-guard
# (``PRODUCTS.get(p.id) is not p``) rely on this object being mutated in place.
PRODUCTS: dict[str, Product] = {}


def get_product(product_id: str) -> Product | None:
    return PRODUCTS.get(product_id)


def iter_products() -> list[Product]:
    # A list, not a view: a concurrent publish would break a caller's loop.
    return list(PRODUCTS.values())


def iter_product_items() -> list[tuple[str, Product]]:
    return list(PRODUCTS.items())


def load_products(new_products: dict[str, Product]) -> None:
    # Additions before removals, so a reader never sees an empty dict.
    if not new_products:
        raise ValueError("Refusing to publish an empty product set")

    for product_id, product in new_products.items():
        PRODUCTS[product_id] = product
    for stale_id in [k for k in PRODUCTS if k not in new_products]:
        del PRODUCTS[stale_id]
    logger.info(f"Published {len(PRODUCTS)} products")
