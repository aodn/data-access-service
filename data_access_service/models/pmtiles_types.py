from abc import ABC
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class TimeGroupBy(str, Enum):
    """Temporal bucket for hexbin count aggregation.

    Feature property prefixes (see ``period_property_key``):
    - DATE  → ``dYYYYMMDD``
    - MONTH → ``mYYYYMM``
    - YEAR  → ``yYYYY``
    - ALL   → day + month + year properties on the same feature
              (staged at day grain; month/year rolled up from day counts)
    """

    MONTH = "month"
    DATE = "date"
    YEAR = "year"
    ALL = "all"


# Single-grain modes that map 1:1 to a property prefix (not ALL).
SINGLE_TIME_GROUP_BY: tuple[TimeGroupBy, ...] = (
    TimeGroupBy.DATE,
    TimeGroupBy.MONTH,
    TimeGroupBy.YEAR,
)


# Synthetic calendar periods for datasets with no TIME column.
# Stable (not generation-time "today"); not real observation times.
TIMELESS_DATE_PERIOD = 19700101  # → property d19700101
TIMELESS_MONTH_PERIOD = 197001  # → property m197001
TIMELESS_YEAR_PERIOD = 1970  # → property y1970

# Prefix for each grain's count properties on vector-tile features.
# ALL is multi-grain and has no single prefix.
PERIOD_PROPERTY_PREFIX: dict[TimeGroupBy, str] = {
    TimeGroupBy.DATE: "d",
    TimeGroupBy.MONTH: "m",
    TimeGroupBy.YEAR: "y",
}


@dataclass(frozen=True)
class PmtilesSidecarMetadata:
    """JSON sidecar written beside a generated ``.pmtiles`` file (``{dname}.metadata``).

    ``has_time`` is False when generation used a synthetic period because the
    source parquet had no TIME column. A real single-day (or single-month)
    archive still has ``has_time=True`` even when ``min_date == max_date``.
    """

    min_date: int
    max_date: int
    time_group_by: TimeGroupBy
    last_updated: str

    # Indicate if the dataset itself have a time component, some data do not have time
    # and in this case this is set to false. Then we use a synthetic date to replace
    # the None and UI need to aware that the date value is created not real.
    has_time: bool = True

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable dict (``time_group_by`` as its string value)."""
        return {
            "min_date": self.min_date,
            "max_date": self.max_date,
            "time_group_by": self.time_group_by.value,
            "has_time": self.has_time,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PmtilesSidecarMetadata":
        # Older sidecars omit has_time; treat as timed (true).
        has_time_raw = data.get("has_time", True)
        if isinstance(has_time_raw, str):
            has_time = has_time_raw.strip().lower() in ("1", "true", "yes")
        else:
            has_time = bool(has_time_raw)
        return cls(
            min_date=int(data["min_date"]),
            max_date=int(data["max_date"]),
            time_group_by=TimeGroupBy(data["time_group_by"]),
            last_updated=str(data["last_updated"]),
            has_time=has_time,
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
    # "month" (YYYYMM), "date" (YYYYMMDD), "year" (YYYY), or "all" (d+m+y);
    # default month.
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
