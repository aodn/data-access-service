"""Derive candidate tiler products from the metadata schema index.

Pure functions of the index, the config entries and the store base URL — no API
instance, no S3, no registry. Dataset names and metadata UUIDs come from the
live metadata snapshot rather than config, so a dataset rename changes the
derived product id instead of leaving a stale one pointing at nothing.

The output is *candidates*; [[verification]] then checks them against the stores
that actually opened.
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

# uuid -> dataset_name -> field names, i.e. what API.get_dataset_variables returns.
SchemaIndex = Mapping[str, Mapping[str, frozenset[str]]]


def product_id(dataset_name: str, variables: list[str]) -> str:
    # Frontend-cached and opaque to ogcapi-java, so this format is a
    # compatibility surface, not just a naming convention.
    return (
        f"{dataset_name.removesuffix('.zarr')}:{'+'.join(v.lower() for v in variables)}"
    )


def source_path(dataset_name: str, base_url: str) -> str:
    # One canonical spelling, no trailing slash: this string keys the store
    # registry, date index, and both cache layers.
    return f"{base_url.rstrip('/')}/{dataset_name}"


def _tile_configs(entry: GriddedVariableEntry, dataset_name: str):
    """Fresh tile config instances for one product, plus its resolved masking.

    Every product must get its *own* instances. DataTileConfig.lod_grids is a
    mutable dict filled in place from that product's store dimensions and never
    recomputed, so a shared instance would make every product fanned out from
    one spec inherit whichever store was requested first — wrong grids
    everywhere, with nothing raised.
    """
    resolved = entry.resolve_defaults(dataset_name)

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
    """Fan every configured specification out across the metadata catalogue.

    A spec matches a dataset when all its variable names are present, matched
    case-sensitively. Raises on a duplicate generated id or an empty result.
    """
    candidates: dict[str, Product] = {}
    origin: dict[str, str] = {}
    matched_entries: set[int] = set()

    # Sorted iteration keeps logs and tests deterministic.
    for uuid in sorted(index):
        for dataset_name in sorted(index[uuid]):
            # The index carries Parquet datasets too; the tiler only opens Zarr.
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
                    # str for a scalar, ordered list for a pair — entry.variables
                    # would turn a scalar into a one-element vector product.
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
    """Report dataset overrides that matched no candidate.

    An override carries a setting a whole-spec default would get wrong (the SLA
    ocean mask), so a key that stops matching — usually an upstream rename —
    means that setting silently stops applying. Logged loudly rather than fatal:
    one stale key should not take the whole catalogue down. Matching is per
    spec, so an override on the UCUR/VCUR entry is unmatched if *that pair*
    produced no product there.
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
