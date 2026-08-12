import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from data_access_service.config.tiler.constants import TILE
from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH

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
    """Fields specific to the /data_tiles pipeline: raw-array chunking/padding
    and coastal inpainting. Not used by /visual_tiles — see
    ``product.DataTileConfig`` for the runtime counterpart this mirrors.

    One shape serves both directions: validating the optional "data_tile"
    block of a gridded_variables.json entry (chunk_px/padding fall back to the
    same defaults Product itself uses when omitted) and serializing it for
    GET /products. extra="forbid" catches config typos.
    """

    model_config = ConfigDict(extra="forbid")

    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFillConfig | None = None


class VisualTileConfig(BaseModel):
    """Fields specific to the /visual_tiles pipeline: independent coastal-fill
    opt-in/tuning from DataTileConfig's — see ``product.VisualTileConfig``
    for the runtime counterpart this mirrors and why it's kept separate.
    """

    model_config = ConfigDict(extra="forbid")

    coastal_fill: CoastalFillConfig | None = None


class ProductConfig(BaseModel):
    """The resolved configuration of a product, serialized for GET /products
    from a live Product (see from_product). extra="forbid" guards it against
    drifting from Product.

    Served identically at both /tiler/data_tiles/products and
    /tiler/visual_tiles/products — data_tile/visual_tile are nested (rather
    than a set of flat fields) so each client can see at a glance which part
    of the payload is its own pipeline's config (e.g. /visual_tiles has no
    chunking/padding concept, so those only ever appear under data_tile).

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
    # Whether /visual_tiles can render this product; ogcapi-java keys its
    # tile_types on it, so it's required rather than defaulted.
    visual: bool
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)

    @classmethod
    def from_product(cls, product: "Product") -> "ProductConfig":
        return cls(
            id=product.id,
            source_path=product.source_path,
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
    """One products.json entry: per-product tuning layered onto a candidate
    discovered by gridded_variables.json + the metadata catalogue (see
    services/product/discovery.py), matched by the derived ``id``
    (``{dataset_name}:{variable(s)}``, see discovery.product_id).

    A candidate with no matching entry here keeps plain defaults
    (ocean_masked=False, visual inferred from arity, default tile configs).
    ``visual: true`` on a pair is rejected at application time, mirroring the
    old per-entry rule now that arity and capability are decided in different
    places. extra="forbid" catches typos.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    ocean_masked: bool | None = None
    visual: bool | None = None
    data_tile: DataTileConfig = Field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = Field(default_factory=VisualTileConfig)


def parse_product_overrides(raw: Any) -> dict[str, ProductOverride]:
    if not isinstance(raw, list):
        raise ValueError(
            f"products.json must contain a JSON array, got {type(raw).__name__}"
        )
    overrides: dict[str, ProductOverride] = {}
    for entry in raw:
        override = ProductOverride.model_validate(entry)
        if override.id in overrides:
            raise ValueError(f"Duplicate products.json override id {override.id!r}")
        overrides[override.id] = override
    return overrides


def load_product_overrides(
    path: str | Path = PRODUCTS_CONFIG_PATH,
) -> dict[str, ProductOverride]:
    return parse_product_overrides(json.loads(Path(path).read_text()))


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
