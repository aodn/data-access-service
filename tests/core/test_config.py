from data_access_service.config.config import Config, EnvType


def test_config_trim():
    config = Config.get_config(EnvType.TESTING)
    # Ensure they are loaded and trimmed correctly
    assert config.get_csv_bucket_name() == "test-bucket"
    assert config.get_wave_buoy_backup_bucket_name() == "test-wave-buoy-backup-bucket"
