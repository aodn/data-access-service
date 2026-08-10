"""Derive candidate tiler products from the metadata schema index.

Dataset names and UUIDs come from live metadata, not config, so a rename changes
the derived id instead of leaving a stale one. [[verification]] then checks
these candidates against the real stores.
"""

import logging
from collections.abc import Mapping

from data_access_service.tiler.schemas.gridded_variables import GriddedVariableEntry
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
    VisualTileConfig,
)

logger = logging.getLogger(__name__)

# uuid -> dataset_name -> field names, as API.get_dataset_variables returns.
SchemaIndex = Mapping[str, Mapping[str, frozenset[str]]]


def product_id(dataset_name: str, variables: list[str]) -> str:
    # Frontend-cached and opaque to ogcapi-java: a compatibility surface.
    return (
        f"{dataset_name.removesuffix('.zarr')}:{'+'.join(v.lower() for v in variables)}"
    )


def source_path(dataset_name: str, base_url: str) -> str:
    # No trailing slash: this string keys the store registry, date index and
    # both cache layers, so a second spelling doubles all of them.
    return f"{base_url.rstrip('/')}/{dataset_name}"


def _tile_configs(entry: GriddedVariableEntry, dataset_name: str):
    # Each product needs its *own* instances: lod_grids is a mutable dict filled
    # in place from that product's store, so sharing one would make every
    # product from a spec inherit whichever store was requested first.
    resolved = entry.settings_for(dataset_name)

    def coastal_fill(config) -> CoastalFill | None:
        return CoastalFill(max_dist_px=config.max_dist_px) if config else None

    return (
        resolved.ocean_masked,
        DataTileConfig(
            chunk_px=resolved.data_tile.chunk_px,
            padding=resolved.data_tile.padding,
            coastal_fill=coastal_fill(resolved.data_tile.coastal_fill),
        ),
        VisualTileConfig(
            coastal_fill=coastal_fill(resolved.visual_tile.coastal_fill),
        ),
    )


def build_candidate_products(
    index: SchemaIndex,
    entries: list[GriddedVariableEntry],
    base_url: str,
) -> dict[str, Product]:
    """Fan each specification out across the catalogue. Matching is case-sensitive."""
    candidates: dict[str, Product] = {}
    origin: dict[str, str] = {}
    matched_entries: set[int] = set()

    # Sorted iteration keeps logs and tests deterministic.
    for uuid in sorted(index):
        for dataset_name in sorted(index[uuid]):
            # The index carries Parquet too; the tiler only opens Zarr.
            if not dataset_name.endswith(".zarr"):
                continue
            fields = index[uuid][dataset_name]

            for position, entry in enumerate(entries):
                variables = entry.variables
                if not all(name in fields for name in variables):
                    continue

                matched_entries.add(position)
                pid = product_id(dataset_name, variables)
                if pid in candidates:
                    raise ValueError(
                        f"Duplicate product id {pid!r} generated from "
                        f"{origin[pid]} and from uuid {uuid} / {dataset_name}"
                    )
                origin[pid] = f"uuid {uuid} / {dataset_name}"

                ocean_masked, data_tile, visual_tile = _tile_configs(
                    entry, dataset_name
                )
                candidates[pid] = Product(
                    id=pid,
                    source_path=source_path(dataset_name, base_url),
                    # Not entry.variables: that would turn a scalar into a
                    # one-element vector product.
                    variable=entry.variable,
                    metadata_uuid=uuid,
                    ocean_masked=ocean_masked,
                    visual=entry.visual,
                    data_tile=data_tile,
                    visual_tile=visual_tile,
                )

    for position, entry in enumerate(entries):
        if position not in matched_entries:
            logger.warning(
                "Variable specification %r matched no dataset in the metadata index",
                entry.variable,
            )

    if not candidates:
        raise ValueError(
            f"No candidate products discovered from {len(entries)} variable "
            "specifications; refusing to publish an empty catalogue"
        )

    logger.info(
        "Discovered %d candidate products across %d stores and %d uuids "
        "from %d variable specifications",
        len(candidates),
        len({p.source_path for p in candidates.values()}),
        len({p.metadata_uuid for p in candidates.values()}),
        len(entries),
    )
    return candidates


def log_unmatched_overrides(
    candidates: Mapping[str, Product],
    entries: list[GriddedVariableEntry],
) -> None:
    """Report overrides that matched no candidate.

    A stale key silently stops its setting applying, so this is loud — but not
    fatal, since one key should not take the catalogue down.
    """
    unmatched = [
        f"{entry.variable} -> {dataset_name}"
        for entry in entries
        for dataset_name in entry.overrides
        if product_id(dataset_name, entry.variables) not in candidates
    ]
    if unmatched:
        logger.error(
            "%d dataset override(s) matched no discovered product, so their "
            "settings will not apply: %s",
            len(unmatched),
            "; ".join(unmatched),
        )
