"""Verify candidate products against the stores that were actually opened.

Discovery matches against *catalogue* metadata; this is where candidates meet
the real store schema. Two O(1) guards per candidate — the variable exists, the
store is time-indexed. Dimensions, dtype and pair shape are deliberately not
checked: the curated variable list is what vouches for renderability today.
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

# Rejection categories, so the startup log can break counts down by cause.
NOT_GRIDDED = "store_not_gridded"
STORE_ABSENT = "store_absent"
VARIABLE_ABSENT = "variable_absent"
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
    # Still published: StoreRegistry.get does not cache the failure, so the
    # first real request re-attempts the open.
    unresolved_stores: list[str] = field(default_factory=list)

    def log_rejections(self) -> None:
        """One line per dropped product, then a breakdown by cause."""
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
            "(store not gridded %d, store absent %d, variable absent %d, "
            "no time dimension %d), %d store(s) unresolved",
            len(self.products),
            len(self.rejections),
            counts[NOT_GRIDDED],
            counts[STORE_ABSENT],
            counts[VARIABLE_ABSENT],
            counts[NO_TIME_DIMENSION],
            len(self.unresolved_stores),
        )


def _guard_failures(product: Product, dataset) -> tuple[str, str] | None:
    """Run the two phase-1 guards. Returns ``(category, reason)`` or None."""
    # Unguarded, catalogue/store drift surfaces later as "No data found for
    # date <date>", which blames the date for a missing variable.
    missing = [name for name in product.variables if name not in dataset]
    if missing:
        return (
            VARIABLE_ABSENT,
            f"variable(s) {', '.join(missing)} absent from the opened store",
        )

    # Without a time dim the date index is empty, so the product would appear
    # in /products and 404 on every date.
    if "time" not in dataset.dims:
        return (
            NO_TIME_DIMENSION,
            "store has no time dimension; every date request would 404",
        )
    return None


def verify_candidate_products(
    candidates: Mapping[str, Product],
    outcomes: Mapping[str, BaseException | None],
) -> VerificationResult:
    """Drop candidates their store cannot support; degrade stores that never opened.

    A store failure is classified by what the store said, never by which
    products sit on it — nothing here takes the tiler down. Rejections are per
    product, so a bad variable leaves its siblings on the same store published.
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
            # Confirmed absence: nothing to recover on a later request.
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
            # The open failed, or reported success with nothing cached — same
            # uncertainty either way.
            unresolved.setdefault(url, []).append(product_id)
            kept[product_id] = product
            continue

        failure = _guard_failures(product, dataset)
        if failure is not None:
            category, reason = failure
            rejections.append(
                Rejection(
                    product_id=product_id,
                    source_path=url,
                    category=category,
                    reason=reason,
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
    """Report stores still unknown after prewarm's retries. Never fatal."""
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
