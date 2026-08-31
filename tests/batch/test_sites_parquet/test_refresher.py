from unittest.mock import MagicMock

from data_access_service.batch.sites_parquet import refresher


class TestRefreshOne:
    def test_calls_load_then_write_snapshot(self):
        repo = MagicMock()
        manager = MagicMock()
        manager.attach_mock(repo.load, "load")
        manager.attach_mock(repo.write_snapshot, "write_snapshot")

        refresher._refresh_one("mooring", repo)

        assert [c[0] for c in manager.mock_calls] == ["load", "write_snapshot"]

    def test_raises_if_load_fails(self):
        repo = MagicMock()
        repo.load.side_effect = RuntimeError("boom")
        try:
            refresher._refresh_one("mooring", repo)
            raise AssertionError("expected RuntimeError to propagate")
        except RuntimeError:
            pass
        repo.write_snapshot.assert_not_called()


class TestRefreshSitesParquetSnapshots:
    def test_refreshes_every_repository_and_closes_session(self, monkeypatch):
        session = MagicMock()
        monkeypatch.setattr(
            refresher, "ParquetDuckDBClient", MagicMock(return_value=session)
        )

        mooring_repo = MagicMock()
        buoy_repo = MagicMock()
        monkeypatch.setattr(
            refresher,
            "build_repositories",
            lambda _s: {"mooring": mooring_repo, "wave-buoy": buoy_repo},
        )

        refresher.refresh_sites_parquet_snapshots()

        mooring_repo.load.assert_called_once()
        mooring_repo.write_snapshot.assert_called_once()
        buoy_repo.load.assert_called_once()
        buoy_repo.write_snapshot.assert_called_once()
        session.close.assert_called_once()

    def test_one_repository_failure_does_not_block_others_or_raise(self, monkeypatch):
        session = MagicMock()
        monkeypatch.setattr(
            refresher, "ParquetDuckDBClient", MagicMock(return_value=session)
        )

        broken_repo = MagicMock()
        broken_repo.load.side_effect = RuntimeError("boom")
        healthy_repo = MagicMock()
        monkeypatch.setattr(
            refresher,
            "build_repositories",
            lambda _s: {"mooring": broken_repo, "wave-buoy": healthy_repo},
        )

        refresher.refresh_sites_parquet_snapshots()  # must not raise

        healthy_repo.load.assert_called_once()
        healthy_repo.write_snapshot.assert_called_once()
        session.close.assert_called_once()
