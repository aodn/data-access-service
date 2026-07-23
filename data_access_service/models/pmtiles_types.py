from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto


class TimeGroupBy(str, Enum):
    """Temporal bucket for hexbin count aggregation."""

    MONTH = "month"
    DATE = "date"


@dataclass(frozen=True)
class PmtilesGenerationConfig:
    output_pmtiles_dir: str
    staged_parquet_dir: str
    geojsonseq_dir: str
    duckdb_temp_dir: str
    duckdb_database: str
    memory_limit: str
    bucket_name: str
    threads: int
    fetch_size: int
    show_progress: bool
    # "month" (YYYYMM) or "date" (YYYYMMDD); default month preserves existing behavior.
    time_group_by: TimeGroupBy = TimeGroupBy.MONTH


@dataclass
class PmtilesLayerSpec(ABC):
    pass


@dataclass
class HexLayerSpec(PmtilesLayerSpec):
    name: str
    h3_resolution: int
    minzoom: int
    maxzoom: int
    layer_geojsonseq_file_name: str


class PmtilesVisualizationStyle(Enum):
    HEXAGONS = auto()
    POINTS = auto()
    LINES = auto()
