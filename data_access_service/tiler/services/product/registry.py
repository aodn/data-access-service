"""In-memory ``Product`` registry, published once at startup by tiler warmup.

Products arrive already derived ([[discovery]]) and verified ([[verification]]).
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


def _assert_no_slice_cache_conflicts(products: dict[str, Product]) -> None:
    """Two products sharing an L1 cache entry must agree on ``ocean_masked``.

    L1 is keyed on ``(source_path, sorted(variables))`` and the mask is applied
    before caching, so disagreement poisons the cache in request order. The sort
    is deliberate — a reversed pair really does share an entry.
    """
    by_identity: dict[tuple[str, tuple[str, ...]], Product] = {}
    for product in products.values():
        identity = (product.source_path, tuple(sorted(product.variables)))
        existing = by_identity.get(identity)
        if existing is None:
            by_identity[identity] = product
        elif existing.ocean_masked != product.ocean_masked:
            raise ValueError(
                f"Products {existing.id!r} and {product.id!r} share the slice-cache "
                f"identity {identity} but disagree on ocean_masked."
            )


def publish_products(new_products: dict[str, Product]) -> None:
    # Additions before removals, so a reader never sees an empty dict.
    if not new_products:
        raise ValueError("Refusing to publish an empty product set")
    _assert_no_slice_cache_conflicts(new_products)

    for product_id, product in new_products.items():
        PRODUCTS[product_id] = product
    for stale_id in [k for k in PRODUCTS if k not in new_products]:
        del PRODUCTS[stale_id]
    logger.info(f"Published {len(PRODUCTS)} products")
