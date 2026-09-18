import dataclasses
from types import SimpleNamespace

import pytest

from data_access_service.config.config import Config, EnvType
from data_access_service.models.co_datasource.csiro.csiro_types import CsiroConfig
from data_access_service.models.tiler_types import TilerConfig


def test_config_trim():
    config = Config.get_config(EnvType.TESTING)
    # Ensure they are loaded and trimmed correctly
    assert config.get_subsetting_bucket_name() == "test-bucket"
    assert config.get_datavis_data_bucket_name() == "test-site-snapshot-bucket"


def test_tiler_parquet_output_dir_requires_s3_prefix(monkeypatch):
    config = Config.get_config(EnvType.TESTING)
    monkeypatch.setitem(config.config, "tiler_parquet", {"config": {}})
    with pytest.raises(ValueError, match="s3_prefix"):
        config.get_tiler_parquet_config()


def test_tiler_parquet_output_dir_composes_s3_uri_from_prefix(monkeypatch):
    config = Config.get_config(EnvType.TESTING)
    monkeypatch.setitem(
        config.config, "tiler_parquet", {"config": {"s3_prefix": "tiler"}}
    )
    assert (
        config.get_tiler_parquet_config().output_dir
        == "s3://test-site-snapshot-bucket/tiler"
    )


def test_pmtiles_use_fork_process_default():
    config = Config.get_config(EnvType.TESTING)
    pm = config.get_pmtiles_config()
    # Base config.yaml defaults to True; tests do not override it.
    assert pm.use_fork_process is True


def test_zarr_chunking_config_from_yaml():
    yaml_cfg = Config.get_config(EnvType.TESTING).config["subsetting"]["config"]
    cfg = Config.get_config(EnvType.TESTING).get_zarr_chunking_config()
    assert cfg.headroom_gb == yaml_cfg["headroom_gb"]
    assert cfg.target_peak_fraction == yaml_cfg["target_peak_fraction"]
    assert cfg.min_chunk_mb == yaml_cfg["min_chunk_mb"]
    assert cfg.memory_fraction == yaml_cfg["memory_fraction"]
    assert cfg.headroom_bytes == int(yaml_cfg["headroom_gb"] * 1024**3)
    assert cfg.min_chunk_bytes == int(yaml_cfg["min_chunk_mb"] * 1024**2)


def test_tiler_co_bucket_defaults_when_absent_from_yaml():
    """Not in config.yaml's tiler section yet (unify with parquet/pmtiles'
    co_bucket later); falls back to the same bucket name they default to,
    with the s3:// scheme this field's consumers need.
    """
    co_bucket = Config.get_config(EnvType.TESTING).get_tiler_config().co_bucket
    assert co_bucket == "s3://aodn-cloud-optimised"
    assert not co_bucket.endswith("/")


# co_bucket is derived (yaml co_bucket + "s3://" prefix) and is_tls is derived
# (True iff CACHE_HOST env var is set) rather than a direct yaml passthrough,
# so neither is expected in yaml at all.
_DERIVED_TILER_FIELDS = {"co_bucket", "is_tls"}
_YAML_TILER_FIELDS = {
    f.name
    for f in dataclasses.fields(TilerConfig)
    if f.name not in _DERIVED_TILER_FIELDS
}

# redis_host is read with .get() (env var CACHE_HOST can override/fill it in),
# so unlike the rest it does not raise KeyError when absent from yaml.
_REQUIRED_TILER_FIELDS = _YAML_TILER_FIELDS - {"redis_host"}


def test_tiler_config_fields_all_come_from_yaml():
    """get_tiler_config constructs TilerConfig field by field — it does not read
    the YAML generically — so a new field has to be declared in three places.
    This is the check that a missed one fails here rather than at first use.
    """
    yaml_keys = set(Config.get_config(EnvType.TESTING).config["tiler"]["config"])
    assert _YAML_TILER_FIELDS == yaml_keys


@pytest.mark.parametrize("missing", sorted(_REQUIRED_TILER_FIELDS))
def test_get_tiler_config_raises_on_missing_yaml_key(missing):
    tiler_section = dict(Config.get_config(EnvType.TESTING).config["tiler"]["config"])
    del tiler_section[missing]
    stub = SimpleNamespace(config={"tiler": {"config": tiler_section}})

    with pytest.raises(KeyError):
        Config.get_tiler_config(stub)


def test_csiro_config_fields_all_come_from_yaml():
    """Same field-by-field construction as the tiler config, same check: a new
    CsiroConfig field must reach the YAML, not just the dataclass."""
    yaml_keys = set(Config.get_config(EnvType.TESTING).config["csiro"])
    assert {f.name for f in dataclasses.fields(CsiroConfig)} == yaml_keys


def test_csiro_urls_are_templates_the_code_can_fill_in():
    """The env overlays only carry `datasets`, so these come from the base
    config.yaml through the deep merge."""
    csiro = Config.get_config(EnvType.TESTING).get_csiro_config()

    assert csiro.collection_url.format(fedora_pid="csiro:1").endswith("/csiro:1")
    assert csiro.key_request_url.format(collection_id=2).endswith("/2/files/s3")
    assert csiro.data_folder == "data/"
    assert csiro.request_timeout_seconds > 0
