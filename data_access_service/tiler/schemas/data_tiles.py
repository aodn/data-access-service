from pydantic import BaseModel


class GeoBounds(BaseModel):
    lonMin: float
    lonMax: float
    latMin: float
    latMax: float


class LodMeta(BaseModel):
    grid: list[int]
    chunkPx: list[int]
    storedPx: list[int]
    padding: int
    # Square-cell NW-anchored geometry (cv2+): one cell size in degrees per LOD,
    # and the LOD grid's own geographic footprint (cell edges). gridBounds can
    # extend past the data on the east/south edge — those pixels are mask-invalid.
    # Clients georeference each LOD by gridBounds, not by the top-level bounds.
    cellSize: float
    gridBounds: GeoBounds
    zoomThreshold: int | None = None


class DataTileManifestResponse(BaseModel):
    bounds: GeoBounds
    lods: dict[str, LodMeta]
    # Scalar products populate valueRange; UV vector products populate uRange + vRange.
    # None fields are excluded from the serialized response via response_model_exclude_none.
    valueRange: list[float] | None = None
    uRange: list[float] | None = None
    vRange: list[float] | None = None
    # Categorical variables (CF flag_values) additionally populate flagValues, and
    # flagMeanings when the labels align 1:1. Absent for continuous products.
    flagValues: list[int] | None = None
    flagMeanings: list[str] | None = None
