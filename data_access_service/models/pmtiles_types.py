from dataclasses import dataclass


@dataclass(frozen=True)
class PmtilesGenerationConfig:
    output_pmtiles: str
    staged_parquet_dir: str
    geojsonseq_dir: str
    duckdb_temp_dir: str
    memory_limit: str
    threads: int
    fetch_size: int


@dataclass(frozen=True)
class HexLayerSpec:
    name: str
    h3_resolution: int
    minzoom: int
    maxzoom: int
    output_path: str
