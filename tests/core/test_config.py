import copy
import dataclasses
from types import SimpleNamespace

import pytest

from data_access_service.config.config import Config, EnvType
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
    assert (
        config.get_tiler_root_dir() == "s3://test-site-snapshot-bucket/tiler_re_layout"
    )
    assert config.get_tiler_batch_config().tiler_root_dir == config.get_tiler_root_dir()


def test_tiler_root_dir_follows_the_configured_prefix(monkeypatch):
    config = Config.get_config(EnvType.TESTING)
    _with_tiler(monkeypatch, config, lambda t: t["config"].update(root_prefix="x/y"))
    assert config.get_tiler_root_dir() == "s3://test-site-snapshot-bucket/x/y"


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


@pytest.mark.parametrize(
    "section, path",
    [("api", p) for p in _leaf_paths(_TILER_SECTION["api"])]
    + [("batch", p) for p in _leaf_paths(_TILER_SECTION["batch"])],
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
