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


def _enable_fork(
    monkeypatch, enabled: bool = True, build_estimation_index: bool = True
):
    monkeypatch.setattr(
        generator.config,
        "get_pmtiles_config",
        lambda: MagicMock(
            use_fork_process=enabled,
            build_estimation_index=build_estimation_index,
            bucket_name="b",
            s3_prefix="portal/visualization",
        ),
    )


@pytest.fixture(autouse=True)
def estimation_phase(monkeypatch):
    """Phase 2 runs for real otherwise: it would fork index workers onto S3."""
    stub = MagicMock()
    monkeypatch.setattr(generator, "generate_estimation_index_for_all_parquets", stub)
    return stub


@pytest.fixture(autouse=True)
def s3(monkeypatch):
    """Fake S3 so the outdated file removal never touches AWS. Returns the client."""
    client = MagicMock()
    monkeypatch.setattr(generator.aws, "s3", client)
    monkeypatch.setattr(generator.aws, "list_all_s3_objects", lambda b, p: [])
    return client


class TestRemoveOutdated:
    """Cleanup at the end of a full run.

    Each dataset listed in the metadata has two files, ``{dataset}.pmtiles`` and
    ``{dataset}.metadata``, in its uuid folder ``portal/visualization/{uuid}/``.
    After the run, every file there that does not belong to a dataset in the
    metadata is deleted. Example, the metadata lists ``uuid-a: a.parquet`` and
    ``uuid-fail: f.parquet``, and f.parquet fails to generate in this run::

        portal/visualization/
          uuid-a/
            a.parquet.pmtiles     kept, refreshed by this run
            a.parquet.metadata    kept, refreshed by this run
            b.parquet.pmtiles     deleted, b.parquet is not in the metadata
            b.parquet.metadata    deleted
          uuid-old/
            old.parquet.pmtiles   deleted, uuid-old is not in the metadata
            old.parquet.metadata  deleted
          uuid-empty/             deleted, an empty uuid folder (key ends in "/")
          uuid-fail/
            f.parquet.pmtiles     kept, f.parquet failed so its last files stay
            f.parquet.metadata    kept

    S3 folders are only key prefixes, so uuid-old and uuid-empty are gone after
    this. There is no cleanup at all when the run is for a single uuid or the
    metadata is empty. A delete error is logged and the run continues.
    One test per case below.
    """

    def _run_batch(self, monkeypatch, s3, metadata, s3_files, uuid=None, ok=True):
        """Run the batch with ``metadata`` loaded and ``s3_files`` in the pmtiles folder. Returns the deleted files."""
        _enable_fork(monkeypatch, True)
        api = MagicMock()
        api.get_mapped_meta_data.return_value = metadata
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", lambda *a: ok
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)
        monkeypatch.setattr(
            generator.aws, "list_all_s3_objects", lambda b, p: list(s3_files)
        )
        generate_pmtiles_for_all_parquets(api, uuid=uuid)
        return [call.kwargs["Key"] for call in s3.delete_object.call_args_list]

    def test_keeps_all_when_nothing_outdated(self, monkeypatch, s3):
        # Every dataset in the metadata is in the run: nothing is deleted
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-a/a.parquet.metadata",
            ],
        )

        assert deleted == []

    def test_deletes_folder_of_removed_uuid(self, monkeypatch, s3):
        # uuid-old is no longer in the metadata: all its dataset files are deleted
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-a/a.parquet.metadata",
                "portal/visualization/uuid-old/old.parquet.pmtiles",
                "portal/visualization/uuid-old/old.parquet.metadata",
            ],
        )

        # Both dataset files go, so the uuid folder goes too (S3 folders are prefixes)
        assert deleted == [
            "portal/visualization/uuid-old/old.parquet.pmtiles",
            "portal/visualization/uuid-old/old.parquet.metadata",
        ]

    def test_deletes_only_files_of_removed_dataset(self, monkeypatch, s3):
        # uuid-a now lists only a.parquet: b.parquet files are deleted, a.parquet stays
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-a/a.parquet.metadata",
                "portal/visualization/uuid-a/b.parquet.pmtiles",
                "portal/visualization/uuid-a/b.parquet.metadata",
            ],
        )

        assert deleted == [
            "portal/visualization/uuid-a/b.parquet.pmtiles",
            "portal/visualization/uuid-a/b.parquet.metadata",
        ]

    def test_deletes_empty_uuid_folder(self, monkeypatch, s3):
        # "Create folder" in the AWS console makes an empty file whose key ends
        # in "/". It is deleted like any other file no dataset in the run owns
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-old/",
            ],
        )

        assert deleted == ["portal/visualization/uuid-old/"]

    def test_keeps_files_of_failed_dataset(self, monkeypatch, s3):
        # a.parquet fails this run: the dataset files it already has stay
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-a/a.parquet.metadata",
                "portal/visualization/uuid-old/old.parquet.pmtiles",
            ],
            ok=False,
        )

        assert deleted == ["portal/visualization/uuid-old/old.parquet.pmtiles"]

    def test_skips_cleanup_for_single_uuid_run(self, monkeypatch, s3):
        # A run for a single uuid never cleans up
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-old/old.parquet.pmtiles",
            ],
            uuid="uuid-a",
        )

        assert deleted == []

    def test_skips_cleanup_when_metadata_empty(self, monkeypatch, s3):
        # Empty metadata means it failed to load, not that every dataset was
        # removed: nothing is deleted
        deleted = self._run_batch(
            monkeypatch,
            s3,
            metadata={},
            s3_files=[
                "portal/visualization/uuid-a/a.parquet.pmtiles",
                "portal/visualization/uuid-old/old.parquet.pmtiles",
            ],
        )

        assert deleted == []

    def test_continues_after_delete_error(self, monkeypatch, s3, estimation_phase):
        # A delete error is logged and the estimation phase still runs
        s3.delete_object.side_effect = RuntimeError("s3 down")

        self._run_batch(
            monkeypatch,
            s3,
            metadata={"uuid-a": {"a.parquet": {}}},
            s3_files=["portal/visualization/uuid-old/old.parquet.pmtiles"],
        )

        estimation_phase.assert_called_once()


class TestBatchProcessIsolation:
    def test_all_parquets_forks_one_child_per_parquet(self, monkeypatch):
        _enable_fork(monkeypatch, True)
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

    def test_all_parquets_runs_in_process_when_fork_disabled(self, monkeypatch):
        _enable_fork(monkeypatch, False)
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a.parquet": {}, "notes.txt": {}},
            "uuid-b": {"b.parquet": {}},
        }
        calls = []
        fork_spy = MagicMock(return_value=True)

        def fake_in_process(passed_api, uuid, dname):
            calls.append((uuid, dname))
            return True

        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets", fake_in_process
        )
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fork_spy
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)

        assert calls == [("uuid-a", "a.parquet"), ("uuid-b", "b.parquet")]
        fork_spy.assert_not_called()

    def test_all_parquets_can_filter_to_single_uuid(self, monkeypatch):
        _enable_fork(monkeypatch, True)
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a.parquet": {}, "notes.txt": {}},
            "uuid-b": {"b.parquet": {}},
        }
        calls = []

        def fake_run(passed_api, uuid, dname):
            calls.append((uuid, dname))
            return True

        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api, uuid="uuid-b")

        assert calls == [("uuid-b", "b.parquet")]
        api.release_memory_for_batch.assert_called_once_with(
            keep_suffix=".parquet", drop_instance=True
        )

    def test_all_parquets_uuid_filter_no_match_skips_workers(self, monkeypatch):
        _enable_fork(monkeypatch, True)
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a.parquet": {}},
        }
        fake_run = MagicMock(return_value=True)
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api, uuid="missing-uuid")

        fake_run.assert_not_called()
        # No work: still avoid trimming? Current code returns before trim when empty.
        api.release_memory_for_batch.assert_not_called()

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
        _enable_fork(monkeypatch, True)
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


class TestEstimationIndexPhase:
    """Phase 2: the index build starts only after every pmtiles child is done."""

    @staticmethod
    def _api():
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a.parquet": {}, "notes.txt": {}},
            "uuid-b": {"b.parquet": {}},
        }
        return api

    def test_runs_once_after_all_pmtiles(self, monkeypatch, estimation_phase):
        _enable_fork(monkeypatch, True)
        api = self._api()
        order = []

        def fake_run(passed_api, uuid, dname):
            order.append(dname)
            return True

        estimation_phase.side_effect = lambda **kwargs: order.append("estimation")
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)

        assert order == ["a.parquet", "b.parquet", "estimation"]
        estimation_phase.assert_called_once_with(api=api, uuid=None)

    def test_passes_the_uuid_filter_through(self, monkeypatch, estimation_phase):
        _enable_fork(monkeypatch, True)
        api = self._api()
        monkeypatch.setattr(
            generator,
            "_generate_pmtiles_for_parquets_in_subprocess",
            lambda *a: True,
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api, uuid="uuid-b")

        estimation_phase.assert_called_once_with(api=api, uuid="uuid-b")

    def test_skipped_when_flag_off(self, monkeypatch, estimation_phase):
        _enable_fork(monkeypatch, True, build_estimation_index=False)
        api = self._api()
        monkeypatch.setattr(
            generator,
            "_generate_pmtiles_for_parquets_in_subprocess",
            lambda *a: True,
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)

        estimation_phase.assert_not_called()

    def test_skipped_when_no_parquet_datasets(self, monkeypatch, estimation_phase):
        _enable_fork(monkeypatch, True)
        api = self._api()
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api, uuid="missing-uuid")

        estimation_phase.assert_not_called()

    def test_failure_does_not_fail_the_pmtiles_run(self, monkeypatch, estimation_phase):
        _enable_fork(monkeypatch, True)
        api = self._api()
        estimation_phase.side_effect = RuntimeError("index boom")
        monkeypatch.setattr(
            generator,
            "_generate_pmtiles_for_parquets_in_subprocess",
            lambda *a: True,
        )
        monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)

        generate_pmtiles_for_all_parquets(api)

        estimation_phase.assert_called_once()


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
            lambda: MagicMock(bucket_name=bucket, s3_prefix="test/visualization"),
        )

        assert _generate_pmtiles_for_parquets(api=None, uuid=uuid, dname=dname) is True
        assert uploaded == [
            (pmtiles_path, bucket, f"test/visualization/{uuid}/{dname}.pmtiles"),
            (metadata_path, bucket, f"test/visualization/{uuid}/{dname}.metadata"),
        ]
