from unittest.mock import MagicMock

import pytest

from data_access_service.batch.pmtiles import generator
from data_access_service.batch.pmtiles.generator import (
    PmtilesGenerationInProgressError,
    _generate_pmtiles_for_parquets,
    generate_pmtiles_for_parquets,
)
from data_access_service.models.pmtiles_types import PmtilesVisualizationStyle


class TestGenerationLock:
    def test_rejects_concurrent_generation(self):
        # Simulate another run in progress by holding the lock.
        assert generator._generation_lock.acquire(blocking=False)
        try:
            with pytest.raises(PmtilesGenerationInProgressError):
                generate_pmtiles_for_parquets(
                    api=None, uuid="uuid-b", dname="b.parquet"
                )
        finally:
            generator._generation_lock.release()

    def test_lock_released_after_successful_run(self, monkeypatch):
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets", lambda api, uuid, dname: True
        )
        assert generate_pmtiles_for_parquets(None, "uuid-a", "a.parquet") is True
        # The lock must be free again for the next run.
        assert generator._generation_lock.acquire(blocking=False)
        generator._generation_lock.release()

    def test_lock_released_when_run_raises(self, monkeypatch):
        def boom(api, uuid, dname):
            raise RuntimeError("dataset exploded")

        monkeypatch.setattr(generator, "_generate_pmtiles_for_parquets", boom)
        with pytest.raises(RuntimeError, match="dataset exploded"):
            generate_pmtiles_for_parquets(None, "uuid-a", "a.parquet")
        assert generator._generation_lock.acquire(blocking=False)
        generator._generation_lock.release()


class TestUploadMetadata:
    def test_uploads_pmtiles_and_metadata_sidecar(self, monkeypatch):
        uuid = "uuid-a"
        dname = "dataset.parquet"
        pmtiles_path = f"/tmp/work/{dname}.pmtiles"
        metadata_path = f"/tmp/work/{dname}.metadata"
        bucket = "test-bucket"

        mock_processor = MagicMock()
        mock_processor.process.return_value = (pmtiles_path, metadata_path)

        monkeypatch.setattr(
            generator,
            "get_visualization_style",
            lambda uuid, dname: PmtilesVisualizationStyle.HEXAGONS,
        )
        monkeypatch.setattr(
            generator, "HexbinProcessor", lambda **kwargs: mock_processor
        )

        uploaded = []

        def capture_upload(file_path, s3_bucket, s3_key):
            uploaded.append((file_path, s3_bucket, s3_key))
            return f"https://example.com/{s3_key}"

        monkeypatch.setattr(generator.aws, "upload_file_to_s3", capture_upload)
        monkeypatch.setattr(
            generator.config,
            "get_pmtiles_config",
            lambda: MagicMock(bucket_name=bucket),
        )

        assert _generate_pmtiles_for_parquets(api=None, uuid=uuid, dname=dname) is True
        assert uploaded == [
            (pmtiles_path, bucket, f"portal/visualization/{uuid}/{dname}.pmtiles"),
            (metadata_path, bucket, f"portal/visualization/{uuid}/{dname}.metadata"),
        ]
