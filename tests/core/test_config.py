from data_access_service.config.config import Config, EnvType


def test_config_trim():
    config = Config.get_config(EnvType.EDGE)
    # Ensure they are loaded and trimmed correctly
    assert config.get_csv_bucket_name() == "aodn-cloud-optimized-subset-edge"
    assert config.get_wave_buoy_backup_bucket_name() == "datavis-apps-edge-data"
