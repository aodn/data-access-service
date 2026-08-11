"""Source config model for ``gridded_variables.json``.

Lists variable specifications, never datasets; startup fans each one out across
the metadata catalogue. Entries are ``"GSLA"``, ``["UCUR", "VCUR"]``, or an
object, all normalised into one canonical form at load.
"""

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from data_access_service.config.tiler.paths import GRIDDED_VARIABLES_CONFIG_PATH
from data_access_service.tiler.schemas.products import DataTileConfig, VisualTileConfig


class ProductSettings(BaseModel):
    """Per-dataset tuning. Anything unset takes the field's own default."""

    model_config = ConfigDict(extra="forbid")

    ocean_masked: bool = False
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)


_NO_OVERRIDE = ProductSettings()


class GriddedVariableEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # str for a scalar, ordered pair for a vector. Pair order is the shader's
    # R/G channel order and must never be sorted.
    variable: str | list[str]
    visual: bool
    # Keyed by the exact metadata dataset name, including .zarr. Tuning is
    # per dataset because it describes one grid, not the variable.
    overrides: dict[str, ProductSettings] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _normalise(cls, data: Any) -> Any:
        # Before field validation, so ``visual`` can stay non-optional.
        if isinstance(data, (str, list)):
            data = {"variable": data}
        if not isinstance(data, dict):
            return data

        data = dict(data)
        variable = data.get("variable")
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
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def settings_for(self, dataset_name: str) -> ProductSettings:
        return self.overrides.get(dataset_name, _NO_OVERRIDE)


def parse_gridded_variables(raw: Any) -> list[GriddedVariableEntry]:
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
    # A missing or empty file is a broken deploy, not an empty state.
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"gridded_variables.json not found at {config_path}")
    return parse_gridded_variables(json.loads(config_path.read_text()))
