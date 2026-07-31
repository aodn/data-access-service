from data_access_service.config.config import Config, EnvType


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
