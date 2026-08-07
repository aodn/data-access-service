import dataclasses
from types import SimpleNamespace

import pytest

from data_access_service.config.config import Config, EnvType
from data_access_service.models.tiler_types import TilerConfig


def test_config_trim():
    config = Config.get_config(EnvType.TESTING)
    # Ensure they are loaded and trimmed correctly
    assert config.get_subsetting_bucket_name() == "test-bucket"
    assert config.get_wave_buoy_backup_bucket_name() == "test-wave-buoy-backup-bucket"


def test_pmtiles_use_fork_process_default():
    config = Config.get_config(EnvType.TESTING)
    pm = config.get_pmtiles_config()
    # Base config.yaml defaults to True; tests do not override it.
    assert pm.use_fork_process is True


def test_tiler_zarr_store_base_url_has_no_trailing_slash():
    """Derived product source paths are built as "<base>/<dataset>", and that
    string is the key for the store registry, date index, and both cache
    layers. A trailing slash here would produce a second spelling of every
    store.
    """
    base_url = Config.get_config(EnvType.TESTING).get_tiler_config().zarr_store_base_url
    assert base_url == "s3://aodn-cloud-optimised"
    assert not base_url.endswith("/")


def test_tiler_config_fields_all_come_from_yaml():
    """get_tiler_config constructs TilerConfig field by field — it does not read
    the YAML generically — so a new field has to be declared in three places.
    This is the check that a missed one fails here rather than at first use.
    """
    yaml_keys = set(Config.get_config(EnvType.TESTING).config["tiler"])
    dataclass_fields = {f.name for f in dataclasses.fields(TilerConfig)}
    assert dataclass_fields == yaml_keys


@pytest.mark.parametrize("missing", [f.name for f in dataclasses.fields(TilerConfig)])
def test_get_tiler_config_raises_on_missing_yaml_key(missing):
    tiler_section = dict(Config.get_config(EnvType.TESTING).config["tiler"])
    del tiler_section[missing]
    stub = SimpleNamespace(config={"tiler": tiler_section})

    with pytest.raises(KeyError):
        Config.get_tiler_config(stub)
