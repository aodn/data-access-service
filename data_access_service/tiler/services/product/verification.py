"""Verify candidate products against the stores that were actually opened.

Discovery matches configured variables against *catalogue* metadata. This module
is where those candidates meet the real store schema, and where a store that
failed to open is classified: confirmed answers drop their candidates, while
operational uncertainty degrades them without taking the tiler down.

Phase 1 runs two O(1) guards per candidate — the variable exists, the store is
time-indexed — plus the store-level lat/lon grid check ``_open_store`` already
performs. It deliberately does not check dimensions, dtype, or pair-shape
compatibility: the curated variable list is the phase-1 authority on
renderability, and per-variable proof is a later step that slots in behind this
same call site.
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

# Rejection categories, so the startup log can break counts down by cause. A
# non-zero VARIABLE_ABSENT count is the one worth stopping on: it means the
# catalogue metadata and the real store schema disagree.
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
    # Stores that stayed operationally unknown after prewarm's bounded retries.
    # Their products are still published: StoreRegistry.get does not cache the
    # failure, so the first real request re-attempts the open and the product
    # recovers on its own.
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
    # Guard 1 — variable presence. Discovery matched against catalogue metadata,
    # not the store's real schema. Unguarded, a drifted product registers
    # cleanly and fails on the first tile request with "No data found for date
    # <date>" — which blames the date for a missing variable, so diagnosis
    # starts from the wrong end.
    missing = [name for name in product.variables if name not in dataset]
    if missing:
        return (
            VARIABLE_ABSENT,
            f"variable(s) {', '.join(missing)} absent from the opened store",
        )

    # Guard 2 — time dimension. _open_store does not require one, and without it
    # the date index is empty, so the product would appear in /products and 404
    # on every single date. The store opens cleanly; the product is just useless.
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
    """Drop candidates their store cannot support; grade stores that never opened.

    No store is special. A store failure is classified by what the store said,
    never by which products happen to sit on it:

      * confirmed non-grid store -> drop every candidate on it;
      * store that does not exist -> drop every candidate on it;
      * unresolved store -> keep its candidates, log at ERROR;
      * opened store -> run the two per-candidate guards.

    Treating any unresolved store as fatal is right at two stores and wrong at
    sixty: one flaky S3 endpoint would take the whole tiler to a permanent 503.
    Unresolved candidates stay registered because StoreRegistry.get does not
    cache the failure, so the first real request re-attempts the open.

    Rejections are per product, so a bad variable on one product leaves its
    siblings on the same store published.
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
            # A confirmed absence, not operational uncertainty: there is
            # nothing to recover on a later request, so the candidates are
            # dropped and named rather than kept in a degraded state that would
            # never resolve.
            rejections.append(
                Rejection(
                    product_id=product_id,
                    source_path=url,
                    category=STORE_ABSENT,
                    reason="store does not exist; the catalogue still advertises it",
                )
            )
            continue

        dataset = None if outcome is not None else cached_store(url)
        if outcome is not None or dataset is None:
            # Operationally unknown: either the open failed, or it reported
            # success but nothing is cached (which is the same uncertainty).
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

    _grade_unresolved_stores(unresolved)

    return VerificationResult(
        products=kept,
        rejections=rejections,
        unresolved_stores=sorted(unresolved),
    )


def _grade_unresolved_stores(unresolved: Mapping[str, list[str]]) -> None:
    """Report stores that stayed operationally unknown after prewarm's retries.

    Never fatal. The products stay registered: ``StoreRegistry.get`` discards the
    per-URL future in ``finally``, so it does not cache the failure and the first
    real request re-attempts the open. Reporting it at ERROR with a count is what
    makes the degradation monitorable rather than silent.
    """
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
