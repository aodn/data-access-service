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


class DataTileManifestResponse(BaseModel):
    bounds: GeoBounds
    lods: dict[str, LodMeta]
    # valueRange for scalar products, uRange + vRange for vector pairs.
    valueRange: list[float] | None = None
    uRange: list[float] | None = None
    vRange: list[float] | None = None
    # Categorical variables only.
    flagValues: list[int] | None = None
    flagMeanings: list[str] | None = None
