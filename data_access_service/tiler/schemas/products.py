from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

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
    cache_version: str


class VariableValue(BaseModel):
    value: float | None
    units: str | None


class PointResponse(BaseModel):
    lat: float
    lon: float
    variables: dict[str, VariableValue]


class VariableInspection(BaseModel):
    dimensions: list[str]
    shape: list[int]
    dtype: str
    # Native Zarr (on-disk) chunk shape in `dimensions` order. None when the store
    # is unchunked (e.g. a contiguous array, or a numpy-backed dataset in tests).
    chunks: list[int] | None = None
    units: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class ProductInspection(BaseModel):
    id: str
    source_path: str
    dimensions: dict[str, int]
    variables: dict[str, VariableInspection]
    attributes: dict[str, Any] = Field(default_factory=dict)
