"""Resolve the live product catalogue from batch-published identities.

``root_metadata.json`` carries only ``ProductIdentity`` (which store, which
variable(s), which metadata collection) — no rendering config. This module
layers ``products_customisation`` on top at load time, so changing that
config only needs a tiler restart, not a batch rerun.
"""

import dataclasses
import logging
from collections.abc import Mapping

from data_access_service.models.tiler_parquet_types import ProductIdentity
from data_access_service.tiler.schemas.products import (
    ProductOverride,
    load_product_overrides,
)
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
    VisualTileConfig,
)

logger = logging.getLogger(__name__)


def _default_product(identity: ProductIdentity) -> Product:
    is_pair = isinstance(identity.variable, list)
    return Product(
        id=identity.id,
        source_path=identity.source_path,
        variable=identity.variable,
        metadata_uuid=identity.metadata_uuid,
        visual=not is_pair,
    )


def _coastal_fill(config) -> CoastalFill | None:
    return CoastalFill(max_dist_px=config.max_dist_px) if config else None


def _apply_override(product: Product, override: ProductOverride | None) -> Product:
    if override is None:
        return product

    visual = product.visual
    if override.visual is not None:
        if len(product.variables) == 2 and override.visual:
            raise ValueError(
                f"products_customisation override {product.id!r} sets visual: true on "
                "a variable pair — visual tiles are single-variable only."
            )
        visual = override.visual

    return dataclasses.replace(
        product,
        visual=visual,
        ocean_masked=(
            override.ocean_masked
            if override.ocean_masked is not None
            else product.ocean_masked
        ),
        data_tile=DataTileConfig(
            chunk_px=override.data_tile.chunk_px,
            padding=override.data_tile.padding,
            coastal_fill=_coastal_fill(override.data_tile.coastal_fill),
        ),
        visual_tile=VisualTileConfig(
            coastal_fill=_coastal_fill(override.visual_tile.coastal_fill),
        ),
    )


def apply_product_overrides(
    candidates: Mapping[str, Product],
    overrides: Mapping[str, ProductOverride],
) -> dict[str, Product]:
    """Layer the products_customisation config onto ``candidates``, matched by
    id. A candidate with no matching override is returned unchanged, at its
    plain defaults.
    """
    return {
        pid: _apply_override(product, overrides.get(pid))
        for pid, product in candidates.items()
    }


def log_unmatched_overrides(
    candidates: Mapping[str, Product],
    overrides: Mapping[str, ProductOverride],
) -> None:
    """Report products_customisation overrides that matched no candidate.

    A stale id silently stops its setting applying, so this is loud — but not
    fatal, since one entry should not take the catalogue down.
    """
    unmatched = [pid for pid in overrides if pid not in candidates]
    if unmatched:
        logger.error(
            "%d products_customisation override(s) matched no discovered product, so "
            "their settings will not apply: %s",
            len(unmatched),
            "; ".join(unmatched),
        )


def build_catalog(identities: Mapping[str, ProductIdentity]) -> dict[str, Product]:
    """Single entry point: turn batch-published identities into the live
    Product catalogue, with products_customisation layered on top by id.
    """
    overrides = load_product_overrides()
    candidates = {
        pid: _default_product(identity) for pid, identity in identities.items()
    }
    log_unmatched_overrides(candidates, overrides)
    return apply_product_overrides(candidates, overrides)
