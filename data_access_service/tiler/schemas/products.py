from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from data_access_service.config.tiler.constants import TILE
from data_access_service.tiler.product import Product


class CoastalFillConfig(BaseModel):
    max_dist_px: int


class DataTileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFillConfig | None = None


class VisualTileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    coastal_fill: CoastalFillConfig | None = None


class ProductConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    source_path: str
    variable: str | list[str]
    metadata_uuid: str | None = None
    ocean_masked: bool
    visual: bool
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)

    @classmethod
    def from_product(cls, product: Product) -> "ProductConfig":
        return cls(
            id=product.id,
            source_path=product.source_path,
            variable=product.variable,
            metadata_uuid=product.metadata_uuid,
            ocean_masked=product.ocean_masked,
            visual=product.visual,
        )


class ProductOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    ocean_masked: bool | None = None
    visual: bool | None = None
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)


def parse_product_overrides(raw: Any) -> dict[str, ProductOverride]:
    if not isinstance(raw, list):
        raise ValueError(
            "config.yaml's tiler.products_customisation must be a list, got "
            f"{type(raw).__name__}"
        )
    overrides: dict[str, ProductOverride] = {}
    for entry in raw:
        override = ProductOverride.model_validate(entry)
        if override.id in overrides:
            raise ValueError(
                f"Duplicate products_customisation override id {override.id!r}"
            )
        overrides[override.id] = override
    return overrides


def load_product_overrides() -> dict[str, ProductOverride]:
    from data_access_service.config.config import Config

    raw = Config.get_tiler_products_customisation() or []
    return parse_product_overrides(raw)


class DateRange(BaseModel):
    start: str | None
    end: str | None


class ProductAvailability(BaseModel):
    available_dates: list[str]
    full_date_range: DateRange


class ManifestResponse(BaseModel):
    products: dict[str, ProductAvailability]


class VariableValue(BaseModel):
    variable: str
    value: float | None


class PointResponse(BaseModel):
    lat: float
    lon: float
    date: str
    values: list[VariableValue]
