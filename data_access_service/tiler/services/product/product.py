import math
import threading
from dataclasses import dataclass, field

from data_access_service.config.tiler.constants import LOD, TILE
from data_access_service.tiler.services.store.registry import get_store

_lod_grids_lock = threading.Lock()


@dataclass(frozen=True)
class CoastalFill:
    """Opt-in coastal-fill config for sparse products (see services/rendering/masks.py).

    ``max_dist_px`` caps how far (in LOD-grid pixels) the nearest-valid inpaint
    reaches past the data edge before the coastline cut. Kept small so we never
    fabricate values far from a real measurement.
    """

    max_dist_px: int


@dataclass(frozen=True)
class Product:
    id: str
    source_path: str
    variable: str | list[str]
    chunk_px: tuple[int, int] = TILE.chunk_px
    padding: int = TILE.padding
    coastal_fill: CoastalFill | None = None
    zoom_thresholds: dict[int, int] = field(
        default_factory=lambda: dict(LOD.zoom_thresholds)
    )
    ocean_masked: bool = False
    # Computed, not settable in products.json — populated lazily from the store's
    # native dimensions on first request (see get_lod_grids below). This is the one
    # field mutated after construction despite frozen=True; guarded by _lod_grids_lock.
    lod_grids: dict[int, tuple[int, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.variable:
            raise ValueError(f"Product '{self.id}' must specify at least one variable")

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

    @property
    def variables(self) -> list[str]:
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def apply_computed_lod_grids(self, data_width: int, data_height: int) -> None:
        """Compute and cache lod_grids from native data dimensions. No-op if already set."""
        if self.lod_grids:
            return
        self.lod_grids.update(
            self._compute_lod_grids(data_width, data_height, self.chunk_px)
        )


def get_lod_grids(product: Product) -> dict[int, tuple[int, int]]:
    """
    Ensure product.lod_grids is populated from actual store dimensions, then return it.
    Writes back to product on first call so subsequent callers find it already set.
    Double-checked locking: fast path avoids lock overhead on every warm call.
    """
    if product.lod_grids:
        return product.lod_grids

    with _lod_grids_lock:
        if product.lod_grids:
            return product.lod_grids

        store = get_store(product.source_path)
        data_height = store.sizes["lat"]
        data_width = store.sizes["lon"]
        product.apply_computed_lod_grids(data_width, data_height)

    return product.lod_grids
