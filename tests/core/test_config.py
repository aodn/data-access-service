import dataclasses
from types import SimpleNamespace

import pytest

from data_access_service.config.config import Config, EnvType
from data_access_service.models.tiler_types import TilerConfig


def test_config_trim():
    config = Config.get_config(EnvType.TESTING)
    # Ensure they are loaded and trimmed correctly
    assert config.get_subsetting_bucket_name() == "test-bucket"
    assert config.get_datavis_data_bucket_name() == "test-site-snapshot-bucket"


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


def test_tiler_vector_config_from_yaml():
    cfg = Config.get_config(EnvType.TESTING).get_tiler_vector_config()
    assert cfg.duckdb_database == ":memory:"
    assert cfg.memory_limit == "128MB"
    assert cfg.max_cells_long_edge == 32
    assert cfg.output_dir == "tiler_vector_out"
    assert cfg.region == "ap-southeast-2"
    assert cfg.s3_prefix == "tiler"
    assert cfg.write_s3 is False
    assert cfg.keep_local_parquet is True
    assert cfg.max_time_slices == 0
    assert cfg.s3_bucket == "test-site-snapshot-bucket"


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
