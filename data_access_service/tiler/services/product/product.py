import math
import threading
from dataclasses import dataclass, field

from data_access_service.config.tiler.constants import LOD, TILE
from data_access_service.tiler.services.store.registry import get_store_metadata

_lod_grids_lock = threading.Lock()


@dataclass(frozen=True)
class CoastalFill:
    """Fill gaps near the coast from the nearest valid value, up to
    ``max_dist_px`` pixels away."""

    max_dist_px: int

    def to_dict(self) -> dict:
        return {"max_dist_px": self.max_dist_px}

    @classmethod
    def from_dict(cls, data: dict) -> "CoastalFill":
        return cls(max_dist_px=int(data["max_dist_px"]))


def _coastal_fill_to_dict(coastal_fill: "CoastalFill | None") -> dict | None:
    return coastal_fill.to_dict() if coastal_fill is not None else None


def _coastal_fill_from_dict(data: "dict | None") -> "CoastalFill | None":
    return CoastalFill.from_dict(data) if data is not None else None


@dataclass(frozen=True)
class DataTileConfig:
    """Data-tile settings: chunking, padding, coastal fill, LOD grids."""

    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFill | None = None
    # Computed on first use (get_lod_grids); filled in place despite frozen.
    lod_grids: dict[int, tuple[int, int]] = field(default_factory=dict)

    @staticmethod
    def _compute_lod_grids(
        data_width: int,
        data_height: int,
        chunk_px: tuple[int, int],
        max_lods: int = LOD.max_lods,
        min_coarsest: tuple[int, int] = LOD.min_coarsest,
    ) -> dict[int, tuple[int, int]]:
        cw, ch = chunk_px
        finest_cols = max(1, math.ceil(data_width / cw))
        finest_rows = max(1, math.ceil(data_height / ch))
        max_depth = (
            math.floor(math.log2(max(finest_cols, finest_rows)))
            if max(finest_cols, finest_rows) > 1
            else 0
        )
        levels = []
        for k in range(max_depth + 1):
            scale = 2**k
            levels.append(
                (
                    max(1, math.ceil(finest_cols / scale)),
                    max(1, math.ceil(finest_rows / scale)),
                )
            )
        levels.reverse()
        min_cols, min_rows = min_coarsest
        levels = [lvl for lvl in levels if lvl[0] >= min_cols and lvl[1] >= min_rows]
        if not levels:
            levels = [(finest_cols, finest_rows)]
        return {i + 1: lvl for i, lvl in enumerate(levels[-max_lods:])}

    def apply_computed_lod_grids(self, data_width: int, data_height: int) -> None:
        """Fill lod_grids from the grid size, if not already set."""
        if self.lod_grids:
            return
        self.lod_grids.update(
            self._compute_lod_grids(data_width, data_height, self.chunk_px)
        )

    def to_dict(self) -> dict:
        # lod_grids is computed, so not serialized.
        return {
            "chunk_px": list(self.chunk_px),
            "padding": self.padding,
            "coastal_fill": _coastal_fill_to_dict(self.coastal_fill),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DataTileConfig":
        chunk_px = data.get("chunk_px")
        return cls(
            chunk_px=tuple(chunk_px) if chunk_px else TILE.chunk_px,
            padding=int(data.get("padding", TILE.padding)),
            coastal_fill=_coastal_fill_from_dict(data.get("coastal_fill")),
        )


@dataclass(frozen=True)
class VisualTileConfig:
    """Visual-tile settings, separate from the data-tile ones."""

    coastal_fill: CoastalFill | None = None

    def to_dict(self) -> dict:
        return {"coastal_fill": _coastal_fill_to_dict(self.coastal_fill)}

    @classmethod
    def from_dict(cls, data: dict) -> "VisualTileConfig":
        return cls(coastal_fill=_coastal_fill_from_dict(data.get("coastal_fill")))


@dataclass(frozen=True)
class Product:
    id: str
    store: str
    variable: str | list[str]
    metadata_uuid: str | None = None
    ocean_masked: bool = False
    # A variable pair is never visual.
    visual: bool = True
    data_tile: DataTileConfig = field(default_factory=DataTileConfig)
    visual_tile: VisualTileConfig = field(default_factory=VisualTileConfig)

    def __post_init__(self) -> None:
        if not self.variable:
            raise ValueError(f"Product '{self.id}' must specify at least one variable")

    @property
    def variables(self) -> list[str]:
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "store": self.store,
            "variable": self.variable,
            "metadata_uuid": self.metadata_uuid,
            "ocean_masked": self.ocean_masked,
            "visual": self.visual,
            "data_tile": self.data_tile.to_dict(),
            "visual_tile": self.visual_tile.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Product":
        return cls(
            id=data["id"],
            store=data["store"],
            variable=data["variable"],
            metadata_uuid=data.get("metadata_uuid"),
            ocean_masked=bool(data.get("ocean_masked", False)),
            visual=bool(data.get("visual", True)),
            data_tile=DataTileConfig.from_dict(data.get("data_tile", {})),
            visual_tile=VisualTileConfig.from_dict(data.get("visual_tile", {})),
        )


def get_lod_grids(product: Product) -> dict[int, tuple[int, int]]:
    """The product's LOD grids, computed from the store's grid on first use."""
    data_tile = product.data_tile
    if data_tile.lod_grids:
        return data_tile.lod_grids

    with _lod_grids_lock:
        if data_tile.lod_grids:
            return data_tile.lod_grids

        meta = get_store_metadata(product.store)
        data_tile.apply_computed_lod_grids(meta.n_j, meta.n_i)

    return data_tile.lod_grids
