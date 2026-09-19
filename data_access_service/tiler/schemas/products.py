from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import TILE

if TYPE_CHECKING:
    from data_access_service.tiler.services.product.product import CoastalFill, Product


class CoastalFillConfig(BaseModel):
    max_dist_px: int


def _coastal_fill_config(
    coastal_fill: "CoastalFill | None",
) -> CoastalFillConfig | None:
    return (
        CoastalFillConfig(max_dist_px=coastal_fill.max_dist_px)
        if coastal_fill
        else None
    )


class DataTileConfig(BaseModel):
    """Data-tile settings: chunking, padding, coastal fill. Used for both the
    config overrides and the /products response."""

    model_config = ConfigDict(extra="forbid")

    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFillConfig | None = None


class VisualTileConfig(BaseModel):
    """Visual-tile settings: coastal fill."""

    model_config = ConfigDict(extra="forbid")

    coastal_fill: CoastalFillConfig | None = None


class ProductConfig(BaseModel):
    """A product as returned by /products. Mirrors ``Product``, minus
    ``lod_grids``."""

    model_config = ConfigDict(extra="forbid")

    id: str
    store: str
    variable: str | list[str]
    # The GeoNetwork/STAC collection UUID.
    metadata_uuid: str | None = None
    ocean_masked: bool
    # Whether visual tiles are available (ogcapi-java relies on it).
    visual: bool
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)

    @classmethod
    def from_product(cls, product: "Product") -> "ProductConfig":
        return cls(
            id=product.id,
            store=product.store,
            variable=product.variable,
            metadata_uuid=product.metadata_uuid,
            ocean_masked=product.ocean_masked,
            visual=product.visual,
            data_tile=DataTileConfig(
                chunk_px=product.data_tile.chunk_px,
                padding=product.data_tile.padding,
                coastal_fill=_coastal_fill_config(product.data_tile.coastal_fill),
            ),
            visual_tile=VisualTileConfig(
                coastal_fill=_coastal_fill_config(product.visual_tile.coastal_fill),
            ),
        )


class ProductOverride(BaseModel):
    """One ``products_customisation`` entry, matched to a product by ``id``.
    Unset fields keep the product's defaults."""

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
    raw = Config.get_config().get_tiler_products_customisation() or []
    return parse_product_overrides(raw)


class DateRange(BaseModel):
    # First and last available date, ignoring from/to. None if no dates.
    start: str | None
    end: str | None


class ProductAvailability(BaseModel):
    available_dates: list[str]
    full_date_range: DateRange


class ManifestResponse(BaseModel):
    products: dict[str, ProductAvailability]


class VariableValue(BaseModel):
    value: float | None
    units: str | None


class PointResponse(BaseModel):
    lat: float
    lon: float
    variables: dict[str, VariableValue]
