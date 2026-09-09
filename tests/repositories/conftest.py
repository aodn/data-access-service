import pytest

from data_access_service.config.config import Config, IntTestConfig
from data_access_service.models.sites_types import SitesConfig


@pytest.fixture(autouse=True)
def memory_parquets_config(monkeypatch):
    """Point SitesDuckDBClient at an in-memory DB.

    SitesDuckDBClient now takes no constructor arguments and reads every
    setting from ``Config.get_sites_config()``. Overriding that here keeps
    these unit tests off disk (no /tmp db file, no .duckdb_temp). The values
    come from ``tests/config/config-test.yaml``'s ``sites:`` section (merged
    over the base ``config.yaml`` via ``IntTestConfig``, same as every other
    test override) rather than being hardcoded here. We can't use
    ``get_sites_config()`` directly since it wraps ``duckdb_database`` in a
    generated temp directory, which would break the ``:memory:`` value.
    httpfs and json are still loaded — both are hardcoded in
    SitesDuckDBClient.get_instance() — but that's a no-op once cached
    locally.
    """
    sconfig = IntTestConfig().config["sites"]["config"]
    cfg = SitesConfig(
        duckdb_database=sconfig["duckdb_database"],
        co_bucket=sconfig["co_bucket"],
        memory_limit=sconfig["memory_limit"],
        threads=sconfig["threads"],
        duckdb_temp_dir=sconfig["duckdb_temp_dir"],
        region=sconfig["region"],
    )
    monkeypatch.setattr(Config, "get_sites_config", lambda self: cfg)
