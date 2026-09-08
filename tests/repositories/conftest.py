import pytest

from data_access_service.config.config import Config
from data_access_service.models.sites_types import SitesConfig


@pytest.fixture(autouse=True)
def memory_parquets_config(monkeypatch):
    """Point SitesDuckDBClient at an in-memory DB.

    SitesDuckDBClient now takes no constructor arguments and reads every
    setting from ``Config.get_sites_config()``. Overriding that here keeps
    these unit tests off disk (no /tmp db file, no .duckdb_temp). httpfs and
    json are still loaded — both are hardcoded in
    SitesDuckDBClient.get_instance() — but that's a no-op once cached locally.
    """
    cfg = SitesConfig(
        duckdb_database=":memory:",
        co_bucket="aodn-cloud-optimised",
        memory_limit="800M",
        threads=8,
        duckdb_temp_dir="/tmp",
        region="ap-southeast-2",
    )
    monkeypatch.setattr(Config, "get_sites_config", lambda self: cfg)
