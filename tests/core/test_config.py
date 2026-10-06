import copy
import dataclasses
from types import SimpleNamespace

import pytest

from data_access_service.config.config import Config, EnvType
from data_access_service.models.cache_types import CacheConfig
from data_access_service.models.tiler_types import TilerBatchVariableCustomisation
from data_access_service.models.co_datasource.csiro.csiro_types import CsiroConfig


def test_config_trim():
    config = Config.get_config(EnvType.TESTING)
    # Ensure they are loaded and trimmed correctly
    assert config.get_subsetting_bucket_name() == "test-bucket"
    assert config.get_datavis_data_bucket_name() == "test-site-snapshot-bucket"


def _with_tiler(monkeypatch, config, edit):
    """Run ``edit`` on a deep copy of the tiler section and install it."""
    tiler = copy.deepcopy(config.config["tiler"])
    edit(tiler)
    monkeypatch.setitem(config.config, "tiler", tiler)


def test_tiler_root_dir_is_the_datavis_bucket():
    config = Config.get_config(EnvType.TESTING)
    assert config.get_tiler_root_dir() == "s3://test-site-snapshot-bucket/shared/tiler"
    assert config.get_tiler_batch_config().tiler_root_dir == config.get_tiler_root_dir()


def test_tiler_root_dir_follows_the_configured_prefix(monkeypatch):
    config = Config.get_config(EnvType.TESTING)
    _with_tiler(monkeypatch, config, lambda t: t["config"].update(root_prefix="x/y"))
    assert config.get_tiler_root_dir() == "s3://test-site-snapshot-bucket/x/y"


def test_tiler_batch_products_customisation_from_yaml():
    customisation = Config.get_config(
        EnvType.TESTING
    ).get_tiler_batch_products_customisation()
    assert customisation["satellite_austemp_heatwave_14day"] == (
        TilerBatchVariableCustomisation(name="dhd", skip_empty_output=True),
    )
    assert (
        Config.get_config(EnvType.TESTING)
        .get_tiler_batch_config()
        .products_customisation
        == customisation
    )


def test_tiler_batch_skip_empty_output_defaults_false(monkeypatch):
    config = Config.get_config(EnvType.TESTING)

    def drop(tiler):
        entry = tiler["config"]["batch"]["products_customisation"][
            "satellite_austemp_heatwave_14day"
        ][0]
        del entry["skip_empty_output"]

    _with_tiler(monkeypatch, config, drop)
    customisation = config.get_tiler_batch_products_customisation()
    assert customisation["satellite_austemp_heatwave_14day"] == (
        TilerBatchVariableCustomisation(name="dhd"),
    )


def test_tiler_batch_max_chunks_null_means_no_limit(monkeypatch):
    config = Config.get_config(EnvType.TESTING)
    _with_tiler(
        monkeypatch,
        config,
        lambda t: t["config"]["batch"].update(max_chunks_per_run=None),
    )
    assert config.get_tiler_batch_config().max_chunks_per_run is None


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
    assert cfg.max_chunk_gb == yaml_cfg["max_chunk_gb"]
    assert cfg.max_chunk_bytes == int(yaml_cfg["max_chunk_gb"] * 1024**3)
    assert cfg.headroom_bytes == int(yaml_cfg["headroom_gb"] * 1024**3)
    assert cfg.min_chunk_bytes == int(yaml_cfg["min_chunk_mb"] * 1024**2)


def _leaf_paths(tree: dict, prefix: tuple = ()) -> list[tuple]:
    paths = []
    for key, value in tree.items():
        if isinstance(value, dict):
            paths += _leaf_paths(value, prefix + (key,))
        else:
            paths.append(prefix + (key,))
    return paths


_TILER_SECTION = Config.get_config(EnvType.TESTING).config["tiler"]["config"]

# Store names under this map are data, not a fixed schema.
_BATCH_SCHEMA = {
    key: value
    for key, value in _TILER_SECTION["batch"].items()
    if key != "products_customisation"
}


@pytest.mark.parametrize(
    "section, path",
    [("api", p) for p in _leaf_paths(_TILER_SECTION["api"])]
    + [("batch", p) for p in _leaf_paths(_BATCH_SCHEMA)],
    ids=lambda v: ".".join(v) if isinstance(v, tuple) else v,
)
def test_tiler_config_raises_on_missing_yaml_key(monkeypatch, section, path):
    """Every tiler key is required: the yaml is the only source of values, so
    a missing one fails at load rather than falling back to a code default."""
    config = Config.get_config(EnvType.TESTING)

    def drop(tiler):
        node = tiler["config"][section]
        for key in path[:-1]:
            node = node[key]
        del node[path[-1]]

    _with_tiler(monkeypatch, config, drop)
    getter = (
        config.get_tiler_api_config
        if section == "api"
        else config.get_tiler_batch_config
    )
    with pytest.raises(KeyError):
        getter()


def test_csiro_config_fields_all_come_from_yaml():
    """CsiroConfig is built field by field, so a new field must reach the YAML,
    not just the dataclass."""
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


# is_tls follows the CACHE_HOST env var rather than the yaml, so it is the one
# CacheConfig field with no key of its own.
_DERIVED_CACHE_FIELDS = {"is_tls"}


def test_cache_config_fields_all_come_from_yaml():
    yaml_keys = set(Config.get_config(EnvType.TESTING).config["cache"])
    fields = {f.name for f in dataclasses.fields(CacheConfig)}
    assert fields - _DERIVED_CACHE_FIELDS == yaml_keys


def test_cache_config_local_default_has_no_tls(monkeypatch):
    monkeypatch.delenv("CACHE_HOST", raising=False)
    cache = Config.get_config(EnvType.TESTING).get_cache_config()

    assert (cache.host, cache.port) == ("localhost", 6379)
    assert cache.is_tls is False


def test_cache_host_env_wins_and_turns_tls_on(monkeypatch):
    """Deployed environments inject the ElastiCache endpoint, which is the only
    one requiring in-transit encryption."""
    monkeypatch.setenv(
        "CACHE_HOST", "das-cache.abc123.serverless.apse2.cache.amazonaws.com"
    )
    cache = Config.get_config(EnvType.TESTING).get_cache_config()

    assert cache.host.endswith(".cache.amazonaws.com")
    assert cache.is_tls is True


@pytest.mark.parametrize("missing", ["backend", "host", "port"])
def test_get_cache_config_raises_on_missing_yaml_key(monkeypatch, missing):
    config = Config.get_config(EnvType.TESTING)
    cache_section = copy.deepcopy(config.config["cache"])
    del cache_section[missing]
    monkeypatch.setitem(config.config, "cache", cache_section)

    with pytest.raises(KeyError):
        config.get_cache_config()
