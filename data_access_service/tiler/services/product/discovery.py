"""Derive candidate tiler products from the metadata schema index.

Dataset names and UUIDs come from live metadata, not config, so a rename changes
the derived id instead of leaving a stale one. Candidates carry plain defaults
only — [[apply_product_overrides]] layers products.json on top by id, as a
separate step, so identity-derivation and config-resolution never mix. 
"""

import dataclasses
import json
import logging
from collections.abc import Iterable, Mapping
from pathlib import Path

from data_access_service.config.tiler.paths import GRIDDED_VARIABLES_CONFIG_PATH
from data_access_service.core.api import API
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


ZarrDatasetVariables = Iterable[tuple[str, str, frozenset[str]]]


GriddedVariableSpec = str | list[str]


def _load_gridded_variable_specs(
    path: str | Path = GRIDDED_VARIABLES_CONFIG_PATH,
) -> list[GriddedVariableSpec]:
    raw = json.loads(Path(path).read_text())
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "gridded_variables.json must contain a non-empty JSON array of variable specs"
        )
    return raw


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
) -> dict[str, Product]:
    """Fan each specification out across the catalogue with plain defaults —
    ocean_masked=False, visual inferred from arity, default tile configs.
    Matching is case-sensitive. Tuning comes later, from
    apply_product_overrides.
    """
    candidates: dict[str, Product] = {}
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

            candidates[pid] = Product(
                id=pid,
                source_path=source_path(dataset_name, base_url),
                # Not `variables`: that would turn a scalar into a
                # one-element vector product.
                variable=list(spec) if is_pair else spec,
                metadata_uuid=uuid,
                visual=not is_pair,
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


def _coastal_fill(config) -> CoastalFill | None:
    return CoastalFill(max_dist_px=config.max_dist_px) if config else None


def _apply_override(product: Product, override: ProductOverride | None) -> Product:
    if override is None:
        return product

    visual = product.visual
    if override.visual is not None:
        if len(product.variables) == 2 and override.visual:
            raise ValueError(
                f"products.json override {product.id!r} sets visual: true on "
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
    """Layer products.json onto discovered candidates, matched by id
    (see product_id). A candidate with no matching override is returned
    unchanged, at its plain defaults.
    """
    return {
        pid: _apply_override(product, overrides.get(pid))
        for pid, product in candidates.items()
    }


def log_unmatched_overrides(
    candidates: Mapping[str, Product],
    overrides: Mapping[str, ProductOverride],
) -> None:
    """Report products.json overrides that matched no candidate.

    A stale id silently stops its setting applying, so this is loud — but not
    fatal, since one entry should not take the catalogue down.
    """
    unmatched = [pid for pid in overrides if pid not in candidates]
    if unmatched:
        logger.error(
            "%d products.json override(s) matched no discovered product, so "
            "their settings will not apply: %s",
            len(unmatched),
            "; ".join(unmatched),
        )


def discover_products(api: API, base_url: str) -> dict[str, Product]:
    """Single entry point: load gridded_variables.json + products.json, fan
    out across the metadata catalogue, and layer overrides on top. Everything
    startup needs from config — no fatal/non-fatal distinction is made here,
    that's up to the caller (run_tiler_warmup treats the whole call as fatal).
    """
    specs = _load_gridded_variable_specs()
    overrides = load_product_overrides()
    candidates = build_candidate_products(
        api.iter_zarr_dataset_variables(), specs, base_url
    )
    log_unmatched_overrides(candidates, overrides)
    return apply_product_overrides(candidates, overrides)
