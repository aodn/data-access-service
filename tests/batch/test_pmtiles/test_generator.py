import json
from unittest.mock import MagicMock

import pytest

from data_access_service.batch.pmtiles import generator
from data_access_service.batch.pmtiles.generator import (
    _generate_pmtiles_for_parquets,
    _generate_pmtiles_for_parquets_in_subprocess,
    generate_pmtiles_for_child,
    generate_pmtiles_for_dataset,
    list_parquet_datasets,
    submit_pmtiles_array_job,
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
        ),
    )


@pytest.fixture(autouse=True)
def estimation_index(monkeypatch):
    """The index runs for real otherwise: it would fork index workers onto S3."""
    stub = MagicMock(return_value=True)
    monkeypatch.setattr(generator, "generate_estimation_index_for_dataset", stub)
    return stub


@pytest.fixture
def no_memory_log(monkeypatch):
    monkeypatch.setattr(generator, "log_memory_usage", lambda *a, **k: None)


class TestBatchProcessIsolation:
    def test_dataset_forks_when_enabled(self, monkeypatch, no_memory_log):
        _enable_fork(monkeypatch, True)
        api = MagicMock()
        fork_run = MagicMock(return_value=True)
        in_process = MagicMock(return_value=True)
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fork_run
        )
        monkeypatch.setattr(generator, "_generate_pmtiles_for_parquets", in_process)

        generate_pmtiles_for_dataset(api, "uuid-a", "a.parquet")

        fork_run.assert_called_once_with(api, "uuid-a", "a.parquet")
        in_process.assert_not_called()

    def test_dataset_runs_in_process_when_fork_disabled(
        self, monkeypatch, no_memory_log
    ):
        _enable_fork(monkeypatch, False)
        api = MagicMock()
        fork_run = MagicMock(return_value=True)
        in_process = MagicMock(return_value=True)
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fork_run
        )
        monkeypatch.setattr(generator, "_generate_pmtiles_for_parquets", in_process)

        generate_pmtiles_for_dataset(api, "uuid-a", "a.parquet")

        in_process.assert_called_once_with(api, "uuid-a", "a.parquet")
        fork_run.assert_not_called()

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


@pytest.mark.usefixtures("no_memory_log")
class TestEstimationIndex:
    """Each dataset's index is built right after its own pmtiles."""

    def test_runs_after_the_pmtiles(self, monkeypatch, estimation_index):
        _enable_fork(monkeypatch, True)
        order = []

        def fake_run(passed_api, uuid, dname):
            order.append(f"pmtiles {dname}")
            return True

        estimation_index.side_effect = lambda a, u, d: order.append(f"index {d}")
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", fake_run
        )

        generate_pmtiles_for_dataset(MagicMock(), "uuid-a", "a.parquet")

        assert order == ["pmtiles a.parquet", "index a.parquet"]

    def test_skipped_when_flag_off(self, monkeypatch, estimation_index):
        _enable_fork(monkeypatch, True, build_estimation_index=False)
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", lambda *a: True
        )

        generate_pmtiles_for_dataset(MagicMock(), "uuid-a", "a.parquet")

        estimation_index.assert_not_called()

    def test_failure_does_not_fail_the_dataset(self, monkeypatch, estimation_index):
        _enable_fork(monkeypatch, True)
        estimation_index.side_effect = RuntimeError("index boom")
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", lambda *a: True
        )

        generate_pmtiles_for_dataset(MagicMock(), "uuid-a", "a.parquet")

        estimation_index.assert_called_once()

    def test_still_runs_when_pmtiles_fails(self, monkeypatch, estimation_index):
        _enable_fork(monkeypatch, True)
        api = MagicMock()
        monkeypatch.setattr(
            generator, "_generate_pmtiles_for_parquets_in_subprocess", lambda *a: False
        )

        # Raising is what makes Batch mark the child FAILED and retry it.
        with pytest.raises(RuntimeError, match="PMTiles failed"):
            generate_pmtiles_for_dataset(api, "uuid-a", "a.parquet")

        estimation_index.assert_called_once_with(api, "uuid-a", "a.parquet")


class TestListParquetDatasets:
    @staticmethod
    def _api():
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-b": {"b.parquet": {}},
            "uuid-a": {"a.zarr": {}, "a.parquet": {}},
        }
        return api

    def test_only_parquet_sorted_by_uuid(self):
        assert list_parquet_datasets(self._api()) == [
            ("uuid-a", "a.parquet"),
            ("uuid-b", "b.parquet"),
        ]

    def test_uuid_filter(self):
        assert list_parquet_datasets(self._api(), uuid="uuid-b") == [
            ("uuid-b", "b.parquet")
        ]


class TestSubmitArrayJob:
    def test_one_array_child_per_dataset(self, monkeypatch):
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {
            "uuid-a": {"a1.parquet": {}, "a2.parquet": {}},
            "uuid-b": {"b.parquet": {}},
        }
        submit = MagicMock(return_value="array-job-id")
        monkeypatch.setattr(generator.aws, "submit_a_job", submit)

        job_id = submit_pmtiles_array_job(
            api, job_queue="queue-arn", job_definition="definition-arn"
        )

        assert job_id == "array-job-id"
        kwargs = submit.call_args.kwargs
        assert kwargs["job_queue"] == "queue-arn"
        assert kwargs["job_definition"] == "definition-arn"
        assert kwargs["array_size"] == 3
        parameters = kwargs["parameters"]
        assert parameters["type"] == "generate-pmtiles-for-parquet-dataset"
        assert json.loads(parameters["datasets"]) == [
            ["uuid-a", "a1.parquet"],
            ["uuid-a", "a2.parquet"],
            ["uuid-b", "b.parquet"],
        ]
        # Batch only takes string parameter values.
        assert all(isinstance(v, str) for v in parameters.values())

    def test_no_dataset_submits_nothing(self, monkeypatch):
        api = MagicMock()
        api.get_mapped_meta_data.return_value = {"uuid-a": {"a.zarr": {}}}
        submit = MagicMock()
        monkeypatch.setattr(generator.aws, "submit_a_job", submit)

        assert submit_pmtiles_array_job(api, job_queue="q", job_definition="d") is None
        submit.assert_not_called()


class TestArrayChild:
    DATASETS = json.dumps([["uuid-a", "a.parquet"], ["uuid-b", "b.parquet"]])

    @pytest.fixture
    def run(self, monkeypatch):
        stub = MagicMock()
        monkeypatch.setattr(generator, "generate_pmtiles_for_dataset", stub)
        return stub

    def test_runs_the_dataset_at_its_index(self, run):
        api = MagicMock()

        generate_pmtiles_for_child(api, {"datasets": self.DATASETS}, job_index="1")

        run.assert_called_once_with(api, "uuid-b", "b.parquet")
        api.release_memory_for_batch.assert_called_once_with(
            keep_uuid="uuid-b", keep_suffix=".parquet", drop_instance=True
        )

    def test_no_index_runs_the_first(self, run):
        # A one-item list is sent as a plain job, which has no array index.
        api = MagicMock()
        datasets = json.dumps([["uuid-a", "a.parquet"]])

        generate_pmtiles_for_child(api, {"datasets": datasets}, job_index=None)

        run.assert_called_once_with(api, "uuid-a", "a.parquet")

    def test_uuid_and_key_run_that_dataset(self, run):
        api = MagicMock()

        generate_pmtiles_for_child(
            api, {"uuid": "uuid-x", "key": "x.parquet"}, job_index="1"
        )

        run.assert_called_once_with(api, "uuid-x", "x.parquet")

    @pytest.mark.parametrize("job_index", ["2", "-1"])
    def test_index_out_of_range_fails(self, run, job_index):
        with pytest.raises(ValueError, match="out of range"):
            generate_pmtiles_for_child(
                MagicMock(), {"datasets": self.DATASETS}, job_index=job_index
            )
        run.assert_not_called()


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
