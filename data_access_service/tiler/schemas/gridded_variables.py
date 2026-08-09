"""Source config model for ``gridded_variables.json``.

The input side of product configuration, separate from [[products]]'s
``ProductConfig`` wire model. The file lists variable specifications only, never
datasets; startup fans each one out across the metadata catalogue.

Two syntaxes per entry, both normalised into one canonical entry at load time:

```json
"GSLA"                                     // scalar, visual defaults true
["UCUR", "VCUR"]                           // ordered pair, visual defaults false
{ "variable": "GSL", "defaults": { ... } } // explicit settings
```

Variable case is preserved verbatim — it is looked up against the real store
schema; lower-casing happens only when building a product ID.
"""

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from data_access_service.config.tiler.paths import GRIDDED_VARIABLES_CONFIG_PATH
from data_access_service.tiler.schemas.products import DataTileConfig, VisualTileConfig


class ProductDefaults(BaseModel):
    """Settings applied to every product a specification fans out to."""

    model_config = ConfigDict(extra="forbid")

    ocean_masked: bool = False
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)


class ProductDefaultsOverride(BaseModel):
    """Only explicitly supplied fields overlay ProductDefaults, recursively.

    Read out via ``model_dump(exclude_unset=True)``, which recurses into nested
    models — so ``{"data_tile": {"padding": 8}}`` overlays only the padding.
    """

    model_config = ConfigDict(extra="forbid")

    ocean_masked: bool | None = None
    data_tile: DataTileConfig | None = None
    visual_tile: VisualTileConfig | None = None


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively overlay ``overlay`` onto ``base``. Non-dict values replace."""
    merged = dict(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if isinstance(value, dict) and isinstance(current, dict):
            merged[key] = _deep_merge(current, value)
        else:
            merged[key] = value
    return merged


class GriddedVariableEntry(BaseModel):
    """One canonical variable specification.

    ``variable`` keeps the configured representation exactly: ``str`` for a
    scalar, ordered two-element ``list[str]`` for a vector pair. Pair order is
    the shader's R/G channel order and must never be sorted.
    """

    model_config = ConfigDict(extra="forbid")

    variable: str | list[str]
    visual: bool
    defaults: ProductDefaults = Field(default_factory=ProductDefaults)
    # Keyed by the exact metadata dataset name, including .zarr. A key matching
    # no discovered dataset is fatal at startup (see discovery).
    overrides: dict[str, ProductDefaultsOverride] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _normalise(cls, data: Any) -> Any:
        """Expand shorthand and resolve the capability default.

        Runs before field validation so ``visual`` can stay non-optional.
        """
        if isinstance(data, (str, list)):
            data = {"variable": data}
        if not isinstance(data, dict):
            return data

        data = dict(data)
        variable = data.get("variable")
        # A pair is data-tile-only: DAS visual tiles render one scalar band.
        is_pair = isinstance(variable, list)
        if data.get("visual") is None:
            data["visual"] = not is_pair
        elif is_pair and data["visual"]:
            raise ValueError(
                f"Variable pair {variable!r} cannot set visual: true — "
                "visual tiles are single-variable only."
            )
        return data

    @model_validator(mode="after")
    def _check_variable(self) -> "GriddedVariableEntry":
        if isinstance(self.variable, str):
            if not self.variable.strip():
                raise ValueError("Variable name must not be blank")
            return self

        if len(self.variable) != 2:
            raise ValueError(
                f"Variable list {self.variable!r} must contain exactly two names; "
                "one is a scalar (use a plain string) and three or more cannot be "
                "encoded into data tiles."
            )
        if any(not isinstance(v, str) or not v.strip() for v in self.variable):
            raise ValueError(f"Variable list {self.variable!r} has a blank name")
        if self.variable[0] == self.variable[1]:
            raise ValueError(
                f"Variable pair {self.variable!r} repeats the same name; "
                "the two channels must be distinct."
            )
        return self

    @property
    def variables(self) -> list[str]:
        """The specification's names in configured order."""
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def resolve_defaults(self, dataset_name: str) -> ProductDefaults:
        """Settings for this specification on ``dataset_name``."""
        override = self.overrides.get(dataset_name)
        if override is None:
            return self.defaults
        merged = _deep_merge(
            self.defaults.model_dump(), override.model_dump(exclude_unset=True)
        )
        return ProductDefaults(**merged)


def parse_gridded_variables(raw: Any) -> list[GriddedVariableEntry]:
    """Validate and normalise the parsed contents of gridded_variables.json."""
    if not isinstance(raw, list):
        raise ValueError(
            f"gridded_variables.json must contain a JSON array, got {type(raw).__name__}"
        )
    if not raw:
        raise ValueError("gridded_variables.json must not be empty")
    return [GriddedVariableEntry.model_validate(entry) for entry in raw]


def load_gridded_variables(
    path: str | Path = GRIDDED_VARIABLES_CONFIG_PATH,
) -> list[GriddedVariableEntry]:
    """Read and validate the committed gridded_variables.json.

    A missing or empty file is a broken deploy, not a legitimate empty state.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"gridded_variables.json not found at {config_path}")
    return parse_gridded_variables(json.loads(config_path.read_text()))
