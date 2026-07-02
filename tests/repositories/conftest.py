import pytest

from data_access_service.config.config import Config
from data_access_service.models.pmtiles_types import ParquetsGenerationConfig


@pytest.fixture(autouse=True)
def memory_parquets_config(monkeypatch):
    """Point ParquetDuckDBClient at an in-memory DB with no extensions.

    ParquetDuckDBClient now takes no constructor arguments and reads every
    setting from ``Config.get_parquets_config()``. Overriding that here keeps
    these unit tests off disk (no /tmp db file, no .duckdb_temp) and off the
    network (no httpfs download).
    """
    cfg = ParquetsGenerationConfig(
        duckdb_database=":memory:",
        memory_limit="800M",
        threads=8,
        duckdb_temp_dir="/tmp",
        region="ap-southeast-2",
        extensions=(),
    )
    monkeypatch.setattr(Config, "get_parquets_config", lambda self: cfg)
