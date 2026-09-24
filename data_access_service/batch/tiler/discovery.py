"""Find the tiler products: config variable specs matched against the zarr
datasets in live metadata. Identity only; rendering config is applied by the
tiler.
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
    raw = Config.get_config().get_tiler_gridded_variables()
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "config.yaml's tiler.catalog.gridded_variables must be a non-empty list of "
            "variable specs"
        )
    return raw


def _load_store_blacklist() -> frozenset[str]:
    raw = Config.get_config().get_tiler_blacklist() or []
    if not isinstance(raw, list):
        raise ValueError(
            "config.yaml's tiler.catalog.blacklist must be a list of store names"
        )
    return frozenset(raw)


def _exclude_blacklisted_stores(
    dataset_variables: ZarrDatasetVariables,
    blacklist: frozenset[str],
) -> ZarrDatasetVariables:
    """Skip datasets whose store name is blacklisted."""
    for uuid, dataset_name, fields in dataset_variables:
        if store_name(dataset_name) in blacklist:
            logger.info("Skipping blacklisted store %r (uuid %s)", dataset_name, uuid)
            continue
        yield uuid, dataset_name, fields


def store_name(dataset_name: str) -> str:
    """``foo.zarr`` -> ``foo``."""
    return dataset_name.removesuffix(".zarr")


def product_id(dataset_name: str, variables: list[str]) -> str:
    return f"{store_name(dataset_name)}:{'+'.join(v.lower() for v in variables)}"


def build_candidate_products(
    dataset_variables: ZarrDatasetVariables,
    specs: list[GriddedVariableSpec],
) -> dict[str, ProductIdentity]:
    """One product per (dataset, spec) where the dataset has every variable
    of the spec. Case-sensitive."""
    candidates: dict[str, ProductIdentity] = {}
    origin: dict[str, str] = {}
    matched_specs: set[int] = set()

    # Sorted so logs and tests are deterministic.
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
                store=store_name(dataset_name),
                # Keep a scalar spec a string, not a one-item list.
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
        len({p.store for p in candidates.values()}),
        len({p.metadata_uuid for p in candidates.values()}),
        len(specs),
    )
    return candidates


def discover_products(api: API) -> dict[str, ProductIdentity]:
    """All products from the config catalog and live metadata."""
    specs = _load_gridded_variable_specs()
    blacklist = _load_store_blacklist()
    dataset_variables = _exclude_blacklisted_stores(
        api.iter_zarr_dataset_variables(), blacklist
    )
    return build_candidate_products(dataset_variables, specs)
