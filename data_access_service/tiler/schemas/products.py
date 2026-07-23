from pydantic import BaseModel
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from data_access_service.tiler.services.product.product import Product


class CoastalFillConfig(BaseModel):
    max_dist_px: int


class ProductConfig(BaseModel):
    """The fields here must match Product's fields, except for lod_grids."""

    id: str
    source_path: str
    variable: str | list[str]
    chunk_px: tuple[int, int]
    padding: int
    coastal_fill: CoastalFillConfig | None = None
    # Links this product to its GeoNetwork/STAC collection UUID. Null when absent,
    # same as coastal_fill.
    metadata_uuid: str | None = None
    ocean_masked: bool

    @classmethod
    def from_product(cls, product: "Product") -> "ProductConfig":
        return cls(
            id=product.id,
            source_path=product.source_path,
            variable=product.variable,
            chunk_px=product.chunk_px,
            padding=product.padding,
            coastal_fill=(
                CoastalFillConfig(max_dist_px=product.coastal_fill.max_dist_px)
                if product.coastal_fill
                else None
            ),
            ocean_masked=product.ocean_masked,
            metadata_uuid=product.metadata_uuid,
        )


class DateRange(BaseModel):
    # Product's full dataset bounds (earliest/latest available date), independent of
    # the from/to filter applied to `available_dates`. Both None when the product has
    # no dates at all.
    start: str | None
    end: str | None


class ProductAvailability(BaseModel):
    available_dates: list[str]
    full_date_range: DateRange


class ManifestResponse(BaseModel):
    products: dict[str, ProductAvailability]
    max_lods: int


class VariableValue(BaseModel):
    value: float | None
    units: str | None


class PointResponse(BaseModel):
    lat: float
    lon: float
    variables: dict[str, VariableValue]
