"""Derive candidate tiler products from the metadata schema index.

Dataset names and UUIDs come from live metadata, not config, so a rename changes
the derived id instead of leaving a stale one. Batch only ever produces
``ProductIdentity`` — which store, which variable(s), which metadata
collection. Rendering config (``visual``/``ocean_masked``/tile configs, from
products_customisation) is resolved on the live tiler side, not here — see
``tiler.services.product.catalog``.
"""

import logging
from collections.abc import Iterable

from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.models.tiler_parquet_types import ProductIdentity

logger = logging.getLogger(__name__)


ZarrDatasetVariables = Iterable[tuple[str, str, frozenset[str]]]


GriddedVariableSpec = str | list[str]


def _load_gridded_variable_specs() -> list[GriddedVariableSpec]:
    raw = Config.get_tiler_gridded_variables()
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "config.yaml's tiler.gridded_variables must be a non-empty list of "
            "variable specs"
        )
    return raw


def _load_store_blacklist() -> frozenset[str]:
    raw = Config.get_tiler_blacklist() or []
    if not isinstance(raw, list):
        raise ValueError("config.yaml's tiler.blacklist must be a list of store names")
    return frozenset(raw)


def _exclude_blacklisted_stores(
    dataset_variables: ZarrDatasetVariables,
    blacklist: frozenset[str],
) -> ZarrDatasetVariables:
    """Drop every (uuid, dataset_name, fields) triple whose store is
    blacklisted, before candidates get fanned out. Matched against the same
    suffix-stripped name product_id uses, so blacklist entries read the same
    as the dataset_name half of a products_customisation id.
    """
    for uuid, dataset_name, fields in dataset_variables:
        if dataset_name.removesuffix(".zarr") in blacklist:
            logger.info("Skipping blacklisted store %r (uuid %s)", dataset_name, uuid)
            continue
        yield uuid, dataset_name, fields


def product_id(dataset_name: str, variables: list[str]) -> str:
    # Frontend-cached and opaque to ogcapi-java: a compatibility surface.
    return (
        f"{dataset_name.removesuffix('.zarr')}:{'+'.join(v.lower() for v in variables)}"
    )


def source_path(dataset_name: str, base_url: str) -> str:
    # No trailing slash: this string keys the store registry, date index and
    # both cache layers, so a second spelling doubles all of them.
    return f"{base_url.rstrip('/')}/{dataset_name}"


def build_candidate_products(
    dataset_variables: ZarrDatasetVariables,
    specs: list[GriddedVariableSpec],
    base_url: str,
) -> dict[str, ProductIdentity]:
    """Fan each specification out across the catalogue. Matching is
    case-sensitive. Carries identity only — no rendering config; that's
    layered on the live tiler side (see ``tiler.services.product.catalog``).
    """
    candidates: dict[str, ProductIdentity] = {}
    origin: dict[str, str] = {}
    matched_specs: set[int] = set()

    # Sorted iteration keeps logs and tests deterministic. Filtering to zarr
    # is API's job now — see iter_zarr_dataset_variables.
    for uuid, dataset_name, fields in sorted(
        dataset_variables, key=lambda t: (t[0], t[1])
    ):
        for position, spec in enumerate(specs):
            is_pair = isinstance(spec, list)
            variables = spec if is_pair else [spec]
            if not all(name in fields for name in variables):
                continue

            matched_specs.add(position)
            pid = product_id(dataset_name, variables)
            if pid in candidates:
                raise ValueError(
                    f"Duplicate product id {pid!r} generated from "
                    f"{origin[pid]} and from uuid {uuid} / {dataset_name}"
                )
            origin[pid] = f"uuid {uuid} / {dataset_name}"

            candidates[pid] = ProductIdentity(
                id=pid,
                source_path=source_path(dataset_name, base_url),
                # Not `variables`: that would turn a scalar into a
                # one-element vector product.
                variable=list(spec) if is_pair else spec,
                metadata_uuid=uuid,
            )

    for position, spec in enumerate(specs):
        if position not in matched_specs:
            logger.warning(
                "Variable specification %r matched no dataset in the metadata index",
                spec,
            )

    if not candidates:
        raise ValueError(
            f"No candidate products discovered from {len(specs)} variable "
            "specifications; refusing to publish an empty catalogue"
        )

    logger.info(
        "Discovered %d candidate products across %d stores and %d uuids "
        "from %d variable specifications",
        len(candidates),
        len({p.source_path for p in candidates.values()}),
        len({p.metadata_uuid for p in candidates.values()}),
        len(specs),
    )
    return candidates


def discover_products(api: API, base_url: str) -> dict[str, ProductIdentity]:
    """Single entry point: load the gridded_variables and blacklist sections
    of config.yaml, and fan out across the metadata catalogue (minus
    blacklisted stores). No rendering config here — the live tiler layers
    products_customisation on top of what this returns (see
    ``tiler.services.product.catalog``).
    """
    specs = _load_gridded_variable_specs()
    blacklist = _load_store_blacklist()
    dataset_variables = _exclude_blacklisted_stores(
        api.iter_zarr_dataset_variables(), blacklist
    )
    return build_candidate_products(dataset_variables, specs, base_url)
