from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from data_access_service.config.tiler.constants import TILE

if TYPE_CHECKING:
    from data_access_service.tiler.services.product.product import Product


class CoastalFillConfig(BaseModel):
    max_dist_px: int


class DataTileConfig(BaseModel):
    """Fields specific to the /data_tiles pipeline: raw-array chunking/padding
    and coastal inpainting. Not used by /visual_tiles — see
    ``product.DataTileConfig`` for the runtime counterpart this mirrors.

    One shape serves both directions: validating the optional "data_tile"
    block in products.json (chunk_px/padding fall back to the same defaults
    Product itself uses when omitted — see registry._from_dict) and
    serializing it for GET /products. extra="forbid" catches config typos.
    """

    model_config = ConfigDict(extra="forbid")

    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFillConfig | None = None


class ProductConfig(BaseModel):
    """The resolved configuration of a product: validated straight from a
    products.json entry on load (see registry._from_dict — id-dependent
    defaults like ocean_masked are resolved just before validation, everything
    else defaults on the model itself), and serialized for GET /products from
    a live Product (see from_product). One shape, two directions —
    extra="forbid" catches config typos on the way in.

    Served identically at both /tiler/data_tiles/products and
    /tiler/visual_tiles/products — data_tile is nested (rather than a set of
    flat fields) specifically so a /visual_tiles client, which has no
    chunking/padding/coastal-fill concept, can see at a glance which part of
    the payload doesn't apply to it.

    Fields here must match Product's fields, except for lod_grids (computed,
    not config).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    source_path: str
    variable: str | list[str]
    # Links this product to its GeoNetwork/STAC collection UUID. Null when absent.
    metadata_uuid: str | None = None
    ocean_masked: bool
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)

    @classmethod
    def from_product(cls, product: "Product") -> "ProductConfig":
        return cls(
            id=product.id,
            source_path=product.source_path,
            variable=product.variable,
            metadata_uuid=product.metadata_uuid,
            ocean_masked=product.ocean_masked,
            data_tile=DataTileConfig(
                chunk_px=product.data_tile.chunk_px,
                padding=product.data_tile.padding,
                coastal_fill=(
                    CoastalFillConfig(
                        max_dist_px=product.data_tile.coastal_fill.max_dist_px
                    )
                    if product.data_tile.coastal_fill
                    else None
                ),
            ),
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


class VariableValue(BaseModel):
    value: float | None
    units: str | None


class PointResponse(BaseModel):
    lat: float
    lon: float
    variables: dict[str, VariableValue]
