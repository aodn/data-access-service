import pytest

from data_access_service.batch.pmtiles import generator
from data_access_service.batch.pmtiles.generator import (
    PmtilesGenerationInProgressError,
    generate_pmtiles_for_parquets,
)


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
