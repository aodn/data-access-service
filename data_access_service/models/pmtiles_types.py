from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto


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
