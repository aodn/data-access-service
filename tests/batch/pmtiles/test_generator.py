from unittest.mock import MagicMock

import pytest

from data_access_service.batch.pmtiles import generator
from data_access_service.batch.pmtiles.generator import (
    PmtilesGenerationInProgressError,
    _generate_pmtiles_for_parquets,
    _generate_pmtiles_for_parquets_in_subprocess,
    generate_pmtiles_for_all_parquets,
    generate_pmtiles_for_parquets,
)
from data_access_service.models.pmtiles_types import PmtilesVisualizationStyle


class TestBatchProcessIsolation:
    def test_all_parquets_forks_one_child_per_parquet(self, monkeypatch):
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {
                "a.parquet": {},
                "notes.txt": {},
            },
            "uuid-b": {
                "b.parquet": {},
            },
        }

        calls = []

        def fake_run(passed_api, uuid, dname):
            calls.append((passed_api, uuid, dname))
            return True

        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)

        assert calls == [
            (api, "uuid-a", "a.parquet"),
            (api, "uuid-b", "b.parquet"),
        ]

    def test_fork_helper_success_exit(self, monkeypatch):
        api = MagicMock()
        recorded = {}

        def fake_fork():
            # Simulate parent side only: never enter the real child branch.
            recorded["forked"] = True
            return 12345

        def fake_waitpid(pid, options):
            recorded["waited_pid"] = pid
            recorded["options"] = options
            # Encode exit status 0 the way waitpid reports it on Linux.
            return pid, 0

        monkeypatch.setattr(generator.os, "fork", fake_fork)
        monkeypatch.setattr(generator.os, "waitpid", fake_waitpid)
        monkeypatch.setattr(generator.os, "WIFEXITED", lambda status: True)
        monkeypatch.setattr(generator.os, "WEXITSTATUS", lambda status: 0)

        assert (
            _generate_pmtiles_for_parquets_in_subprocess(api, "uuid-x", "ds.parquet")
            is True
        )
        assert recorded["forked"] is True
        assert recorded["waited_pid"] == 12345

    def test_fork_helper_nonzero_exit(self, monkeypatch):
        api = MagicMock()

        monkeypatch.setattr(generator.os, "fork", lambda: 99)
        monkeypatch.setattr(generator.os, "waitpid", lambda pid, options: (pid, 1))
        monkeypatch.setattr(generator.os, "WIFEXITED", lambda status: True)
        monkeypatch.setattr(generator.os, "WEXITSTATUS", lambda status: 1)

        assert (
            _generate_pmtiles_for_parquets_in_subprocess(api, "uuid-x", "ds.parquet")
            is False
        )

    def test_fork_helper_child_runs_generation_and_exits(self, monkeypatch):
        """Real fork: child inherits api, runs generation, exits with its return value."""
        api = MagicMock()
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets", lambda a, u, d: True
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        assert (
            _generate_pmtiles_for_parquets_in_subprocess(api, "uuid-x", "ds.parquet")
            is True
        )

    def test_fork_helper_child_failure_return_code(self, monkeypatch):
        api = MagicMock()
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets", lambda a, u, d: False
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        assert (
            _generate_pmtiles_for_parquets_in_subprocess(api, "uuid-x", "ds.parquet")
            is False
        )

    def test_batch_continues_after_failed_child(self, monkeypatch):
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a.parquet": {}, "b.parquet": {}},
        }
        results = iter([False, True])
        calls = []

        def fake_run(passed_api, uuid, dname):
            calls.append(dname)
            return next(results)

        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)
        assert calls == ["a.parquet", "b.parquet"]


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
