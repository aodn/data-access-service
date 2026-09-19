"""The in-memory product catalogue, loaded at startup. Store health is
checked per request (``store.registry``), not here."""

import logging

from data_access_service.tiler.services.product.product import Product

logger = logging.getLogger(__name__)

# Updated in place, never replaced: callers hold a reference to it.
PRODUCTS: dict[str, Product] = {}


def get_product(product_id: str) -> Product | None:
    return PRODUCTS.get(product_id)


def iter_products() -> list[Product]:
    # A copy, so a concurrent update can't break the caller's loop.
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
