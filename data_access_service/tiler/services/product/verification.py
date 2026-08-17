"""Verify candidate products against the stores that were actually opened.

Variable presence isn't checked here — catalogue/store disagreements observed
in practice have always been at store level (not gridded, absent), which
prewarm already classifies.
"""

import logging
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field

from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.store.registry import (
    NotGriddedStoreError,
    cached_store,
)

logger = logging.getLogger(__name__)

NOT_GRIDDED = "store_not_gridded"
STORE_ABSENT = "store_absent"
NO_TIME_DIMENSION = "no_time_dimension"


@dataclass(frozen=True)
class Rejection:
    product_id: str
    source_path: str
    category: str
    reason: str


@dataclass(frozen=True)
class VerificationResult:
    products: dict[str, Product] = field(default_factory=dict)
    rejections: list[Rejection] = field(default_factory=list)
    # Still published: the failure is not cached, so a later request retries.
    unresolved_stores: list[str] = field(default_factory=list)

    def log_rejections(self) -> None:
        for rejection in self.rejections:
            logger.info(
                "Rejected product %s on %s: %s",
                rejection.product_id,
                rejection.source_path,
                rejection.reason,
            )
        counts = Counter(r.category for r in self.rejections)
        logger.info(
            "Verification: %d products kept, %d rejected "
            "(store not gridded %d, store absent %d, no time dimension %d), "
            "%d store(s) unresolved",
            len(self.products),
            len(self.rejections),
            counts[NOT_GRIDDED],
            counts[STORE_ABSENT],
            counts[NO_TIME_DIMENSION],
            len(self.unresolved_stores),
        )


def verify_candidate_products(
    candidates: Mapping[str, Product],
    outcomes: Mapping[str, BaseException | None],
) -> VerificationResult:
    """Drop candidates their store cannot support; degrade stores that never opened.

    Classified by what the store said, never by which products sit on it, so
    nothing here takes the tiler down. Rejections are per product.
    """
    kept: dict[str, Product] = {}
    rejections: list[Rejection] = []
    unresolved: dict[str, list[str]] = {}

    for product_id, product in candidates.items():
        url = product.source_path
        if url not in outcomes:
            raise ValueError(
                f"No prewarm outcome recorded for {url!r} (product {product_id}); "
                "prewarm must be given every candidate's source path."
            )
        outcome = outcomes[url]

        if isinstance(outcome, NotGriddedStoreError):
            rejections.append(
                Rejection(
                    product_id=product_id,
                    source_path=url,
                    category=NOT_GRIDDED,
                    reason="store is not a lat/lon grid",
                )
            )
            continue

        if isinstance(outcome, FileNotFoundError):
            rejections.append(
                Rejection(
                    product_id=product_id,
                    source_path=url,
                    category=STORE_ABSENT,
                    reason="store does not exist; the catalogue still advertises it",
                )
            )
            continue

        dataset = cached_store(url) if outcome is None else None
        if dataset is None:
            unresolved.setdefault(url, []).append(product_id)
            kept[product_id] = product
            continue

        # No time dim means an empty date index: 404 on every date.
        if "time" not in dataset.dims:
            rejections.append(
                Rejection(
                    product_id=product_id,
                    source_path=url,
                    category=NO_TIME_DIMENSION,
                    reason="store has no time dimension; every date request would 404",
                )
            )
            continue

        kept[product_id] = product

    _log_unresolved_stores(unresolved)

    return VerificationResult(
        products=kept,
        rejections=rejections,
        unresolved_stores=sorted(unresolved),
    )


def _log_unresolved_stores(unresolved: Mapping[str, list[str]]) -> None:
    if not unresolved:
        return

    degraded = sum(len(ids) for ids in unresolved.values())
    logger.error(
        "%d store(s) remain unresolved after prewarm, degrading %d product(s): %s. "
        "They stay registered and recover on the first request that reopens the store.",
        len(unresolved),
        degraded,
        ", ".join(sorted(unresolved)),
    )
