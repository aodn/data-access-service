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


@dataclass(frozen=True)
class ParquetsGenerationConfig:
    """DuckDB tuning for the API's on-disk Parquet client.

    The sibling of :class:`PmtilesGenerationConfig` for the read path. It carries
    everything
    :class:`~data_access_service.core.duckdbclient.ParquetDuckDBClient` needs to
    build its connection — database path, memory limit, thread count, spill
    (temp) directory, S3 region, and the extensions to load — so the client
    takes no constructor arguments (tests override this config instead).
    """

    database: str
    memory_limit: str
    threads: int
    temp_directory: str
    region: str
    extensions: tuple[str, ...]


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
