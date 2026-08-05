"""Derive candidate tiler products from the metadata schema index.

Everything here is a pure function of its arguments — the schema index, the
normalised config entries, and the store base URL. It never touches the ``API``
instance, S3, or the product registry, which is what makes the fan-out fully
unit-testable and keeps the one genuinely I/O-bound step (opening the stores)
in startup where it belongs.

The output is *candidates*: products whose variables the catalogue metadata says
exist. Verification against the real opened stores happens afterwards, in
[[verification]] — a candidate is not a published product.

Dataset names and metadata UUIDs come from the live metadata snapshot rather
than from config, which is the whole point of the redesign: a dataset rename
changes the derived product ID instead of leaving a stale one pointing at
nothing.
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

# uuid -> dataset_name -> top-level metadata field names, i.e. exactly what
# API.get_dataset_variables(None) returns.
SchemaIndex = Mapping[str, Mapping[str, frozenset[str]]]


def product_id(dataset_name: str, variables: list[str]) -> str:
    """Build the public product ID for a dataset/variable combination.

    ``{zarr name}:{variables joined by +}``, lower-cased on the variable side
    only. These IDs are cached by the frontend and treated as opaque by
    ogcapi-java, so the formula is a compatibility surface: the five products
    that predate derivation must come out of it byte for byte.
    """
    return (
        f"{dataset_name.removesuffix('.zarr')}:{'+'.join(v.lower() for v in variables)}"
    )


def source_path(dataset_name: str, base_url: str) -> str:
    """Canonical store URL for a dataset name. No trailing slash, ever.

    Single source of this string: it keys the store registry, date index, slice
    cache, processed cache, and dedupe, so discovery deriving one spelling and
    the store layer another would quietly double every one of them.
    """
    return f"{base_url.rstrip('/')}/{dataset_name}"


def _tile_configs(entry: GriddedVariableEntry, dataset_name: str):
    """Fresh runtime tile configs for one product, plus its resolved masking.

    Every product gets its *own* DataTileConfig/VisualTileConfig instance. This
    is a correctness rule, not tidiness: DataTileConfig.lod_grids is a mutable
    dict on a frozen dataclass, filled in place on first request from that
    product's own store dimensions and never recomputed. Sharing one instance
    across a fan-out would make all 32 sea_surface_temperature products — which
    sit on differently-sized grids — inherit whichever store was requested
    first, wrong in /manifest and in every data tile, with nothing raised.
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

    A specification matches a dataset when every one of its variable names is
    present in that dataset's metadata fields, matched case-sensitively — the
    names are looked up verbatim in the real store later, and the catalogue
    already distinguishes ``sst`` from ``SST``.

    Raises on a duplicate generated ID or an empty result; both are deployment
    errors rather than states worth booting into.
    """
    candidates: dict[str, Product] = {}
    # Which config entry produced each ID, so a duplicate can name both sides.
    origin: dict[str, str] = {}
    matches_per_entry: list[int] = [0] * len(entries)

    # Sorted iteration keeps logs, tests, and the rejection ordering deterministic.
    for uuid in sorted(index):
        for dataset_name in sorted(index[uuid]):
            # The index carries Parquet datasets too; the tiler only ever opens
            # Zarr, so they are excluded before any variable is looked at.
            if not dataset_name.endswith(".zarr"):
                continue
            fields = index[uuid][dataset_name]

            for position, entry in enumerate(entries):
                variables = entry.variables
                if not all(name in fields for name in variables):
                    continue

                matches_per_entry[position] += 1
                pid = product_id(dataset_name, variables)
                if pid in candidates:
                    raise ValueError(
                        f"Duplicate product id {pid!r} generated from "
                        f"{origin[pid]} and from uuid {uuid} / {dataset_name}. "
                        "A product id maps to exactly one collection and one "
                        "resolved configuration."
                    )
                origin[pid] = f"uuid {uuid} / {dataset_name}"

                ocean_masked, data_tile, visual_tile = _tile_configs(
                    entry, dataset_name
                )
                candidates[pid] = Product(
                    id=pid,
                    source_path=source_path(dataset_name, base_url),
                    # Preserve the configured representation exactly: str for a
                    # scalar, ordered list for a pair. entry.variables always
                    # returns a list, which would turn a scalar into a
                    # one-element vector product.
                    variable=entry.variable,
                    metadata_uuid=uuid,
                    ocean_masked=ocean_masked,
                    visual=entry.visual,
                    data_tile=data_tile,
                    visual_tile=visual_tile,
                )

    for entry, matched in zip(entries, matches_per_entry):
        if matched == 0:
            # Not fatal on its own: a variable can legitimately disappear from
            # the catalogue ahead of the config being trimmed. Worth a warning
            # because it is equally often a rename or a typo.
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


def unmatched_overrides(
    candidates: Mapping[str, Product],
    entries: list[GriddedVariableEntry],
) -> list[tuple[str, str]]:
    """Return ``(variable spec, dataset key)`` for every override that matched nothing.

    Matching is per specification, not global: an override on the ``UCUR``/
    ``VCUR`` entry is unmatched if that pair produced no product on the named
    dataset, even when some other specification did.
    """
    unmatched: list[tuple[str, str]] = []
    for entry in entries:
        for dataset_name in entry.overrides:
            expected = product_id(dataset_name, entry.variables)
            if expected not in candidates:
                unmatched.append((str(entry.variable), dataset_name))
    return unmatched


def reject_unmatched_overrides(
    candidates: Mapping[str, Product],
    entries: list[GriddedVariableEntry],
) -> None:
    """Fail startup on a dataset override that matched no candidate.

    An override exists to carry a correctness setting a whole-specification
    default would get wrong — the SLA ocean mask is the live example. If its key
    stops matching, usually because of an upstream rename, the setting silently
    disappears and the product renders wrong rather than failing. So it is fatal.
    """
    unmatched = unmatched_overrides(candidates, entries)
    if not unmatched:
        return
    detail = "; ".join(
        f"{variable} -> {dataset_name}" for variable, dataset_name in unmatched
    )
    raise ValueError(
        f"{len(unmatched)} dataset override(s) matched no discovered product: "
        f"{detail}. An override key must name a dataset the same specification "
        "actually produced a product for."
    )
