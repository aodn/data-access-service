from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class TimeGroupBy(str, Enum):
    """Temporal bucket for hexbin count aggregation."""

    MONTH = "month"
    DATE = "date"


@dataclass(frozen=True)
class PmtilesSidecarMetadata:
    """JSON sidecar written beside a generated ``.pmtiles`` file (``{dname}.metadata``)."""

    min_date: int
    max_date: int
    time_group_by: TimeGroupBy
    last_updated: str

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable dict (``time_group_by`` as its string value)."""
        return {
            "min_date": self.min_date,
            "max_date": self.max_date,
            "time_group_by": self.time_group_by.value,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PmtilesSidecarMetadata":
        return cls(
            min_date=int(data["min_date"]),
            max_date=int(data["max_date"]),
            time_group_by=TimeGroupBy(data["time_group_by"]),
            last_updated=str(data["last_updated"]),
        )


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
    # When True (default), batch generation forks one short-lived child per
    # parquet dataset so DuckDB/tippecanoe memory returns to the OS on exit.
    # When False, each dataset runs in the main process (useful for local
    # debug or agents that do not tolerate os.fork, e.g. some APM agents).
    use_fork_process: bool = True


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
