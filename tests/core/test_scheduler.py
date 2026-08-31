from unittest.mock import AsyncMock, MagicMock

import pytest

from data_access_service.config.config import Config, EnvType
from data_access_service.core.scheduler import TaskScheduler


def _make_repo(reload_result: bool = True):
    repo = MagicMock()
    repo.reload_if_changed.return_value = reload_result
    return repo


class TestReloadRepository:
    def test_refreshes_snapshot_secret_then_reloads(self, monkeypatch):
        monkeypatch.setattr(Config, "is_profile_in", lambda *a, **k: True)
        repo = _make_repo(reload_result=True)
        scheduler = TaskScheduler(api=MagicMock(), repositories={"mooring": repo})

        manager = MagicMock()
        manager.attach_mock(repo._configure_snapshot_bucket_s3, "configure")
        manager.attach_mock(repo.reload_if_changed, "reload")

        scheduler._reload_repository("mooring", repo)

        assert [c[0] for c in manager.mock_calls] == ["configure", "reload"]

    def test_never_touches_primary_bucket_secret(self):
        repo = _make_repo()
        scheduler = TaskScheduler(api=MagicMock(), repositories={"mooring": repo})
        scheduler._reload_repository("mooring", repo)
        repo._configure_s3.assert_not_called()

    def test_does_not_raise_when_reload_fails(self):
        repo = _make_repo()
        repo.reload_if_changed.side_effect = RuntimeError("boom")
        scheduler = TaskScheduler(api=MagicMock(), repositories={"mooring": repo})
        scheduler._reload_repository("mooring", repo)  # must not raise


class TestReloadTask:
    def test_reloads_every_repository_when_profile_allowed(self, monkeypatch):
        monkeypatch.setattr(Config, "is_profile_in", lambda *a, **k: True)
        mooring = _make_repo()
        buoy = _make_repo()
        scheduler = TaskScheduler(
            api=MagicMock(), repositories={"mooring": mooring, "wave-buoy": buoy}
        )

        scheduler._reload_task()

        mooring.reload_if_changed.assert_called_once()
        buoy.reload_if_changed.assert_called_once()

    def test_skips_all_repositories_on_disallowed_profile(self, monkeypatch):
        monkeypatch.setattr(Config, "is_profile_in", lambda *a, **k: False)
        monkeypatch.setattr(Config, "resolve_profile", lambda: EnvType.DEV)
        repo = _make_repo()
        scheduler = TaskScheduler(api=MagicMock(), repositories={"mooring": repo})

        scheduler._reload_task()

        repo.reload_if_changed.assert_not_called()

    def test_one_repository_failure_does_not_block_others(self, monkeypatch):
        monkeypatch.setattr(Config, "is_profile_in", lambda *a, **k: True)
        broken = _make_repo()
        broken.reload_if_changed.side_effect = RuntimeError("boom")
        healthy = _make_repo()
        scheduler = TaskScheduler(
            api=MagicMock(), repositories={"mooring": broken, "wave-buoy": healthy}
        )

        scheduler._reload_task()  # must not raise

        healthy.reload_if_changed.assert_called_once()


class TestStartWithInitialRun:
    @pytest.mark.asyncio
    async def test_waits_for_api_then_reloads_then_starts_scheduler(
        self, monkeypatch
    ):
        monkeypatch.setattr(Config, "is_profile_in", lambda *a, **k: True)
        api = MagicMock()
        api.wait_until_ready = AsyncMock()
        repo = _make_repo()
        scheduler = TaskScheduler(api=api, repositories={"mooring": repo})
        scheduler._start = MagicMock()

        await scheduler.start_with_initial_run()

        api.wait_until_ready.assert_awaited_once()
        repo.reload_if_changed.assert_called_once()
        scheduler._start.assert_called_once()


class TestStart:
    def test_registers_hourly_cron_job(self):
        scheduler = TaskScheduler(api=MagicMock(), repositories={})
        scheduler.scheduler = MagicMock()

        scheduler._start()

        scheduler.scheduler.add_job.assert_called_once()
        kwargs = scheduler.scheduler.add_job.call_args.kwargs
        fields = {f.name: str(f) for f in kwargs["trigger"].fields}
        assert fields["minute"] == "0"  # on the hour
        assert fields["hour"] == "*"  # every hour
        scheduler.scheduler.start.assert_called_once()


class TestShutdown:
    def test_shuts_down_running_scheduler(self):
        scheduler = TaskScheduler(api=MagicMock(), repositories={})
        scheduler.scheduler = MagicMock()
        scheduler.scheduler.running = True

        scheduler.shutdown()

        scheduler.scheduler.shutdown.assert_called_once_with(wait=True)

    def test_does_nothing_when_scheduler_not_running(self):
        scheduler = TaskScheduler(api=MagicMock(), repositories={})
        scheduler.scheduler = MagicMock()
        scheduler.scheduler.running = False

        scheduler.shutdown()

        scheduler.scheduler.shutdown.assert_not_called()
