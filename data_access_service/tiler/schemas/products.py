from typing import Any

from pydantic import BaseModel, Field


class CoastalFillConfig(BaseModel):
    max_dist_px: int


class ProductConfig(BaseModel):
    id: str
    source_path: str
    variable: str | list[str]
    # Present in the response only when the product enables coastal fill (see
    # §7.6); omitted otherwise via response_model_exclude_none on GET /products.
    coastal_fill: CoastalFillConfig | None = None


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


class CoverageGeoBounds(BaseModel):
    lonMin: float
    lonMax: float
    latMin: float
    latMax: float


class CoverageLodMeta(BaseModel):
    grid: list[int]
    # Square-cell NW-anchored geometry (grid_geometry.py): one cell size in
    # degrees; gridBounds is the LOD grid's own footprint (cell edges), which may
    # extend past the data on the east/south — those pixels are mask-invalid.
    cellSize: float
    gridBounds: CoverageGeoBounds
    zoomThreshold: int | None = None


class CoverageTimeMapping(BaseModel):
    # RFC 3339 instant (the timestamp DAS selects for the local date) and the
    # DAS-internal date key it maps to. OGC `datetime` must match `datetime`
    # exactly; tile URLs use `dateKey`.
    datetime: str
    dateKey: str


class CoverageProductEntry(BaseModel):
    id: str
    title: str
    datasetKey: str
    default: bool
    variables: list[str]
    encoding: str  # scalar-rgb24 | vector-rg8
    maskChannel: str  # A (scalar) | B (vector)
    tileMatrixSetId: str
    bounds: CoverageGeoBounds  # data extent (cell centres)
    chunkPx: list[int]
    storedPx: list[int]
    padding: int
    lods: dict[str, CoverageLodMeta]
    times: list[CoverageTimeMapping]
    flagValues: list[int] | None = None
    flagMeanings: list[str] | None = None


class CoverageDiscoveryResponse(BaseModel):
    """Browser-safe coverage discovery for one portal collection — the input to
    ogcapi-java's coverage catalog. Never contains source paths or store URLs."""

    collectionId: str
    cacheVersion: str
    products: list[CoverageProductEntry]


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
