"""Time-chunk sizing for the zarr subset write."""

from unittest.mock import MagicMock

import dask.array as da
import pytest
import xarray as xr

from data_access_service.batch.subsetting.helpers import zarr_chunking
from data_access_service.batch.subsetting.helpers.zarr_chunking import (
    get_time_steps_per_chunk,
)


def _safe_block_bytes(total: int, rss: int) -> int:
    """Same ceiling the sizer uses: host peak or max_chunk_gb, minus RSS, halved."""
    cfg = zarr_chunking._chunking_config()
    target_peak = min(total - cfg.headroom_bytes, int(total * cfg.target_peak_fraction))
    ceiling = min(target_peak, cfg.max_chunk_bytes)
    room = max(0, ceiling - rss)
    return max(cfg.min_chunk_bytes, room // 2)


def _dataset(n_time: int) -> xr.Dataset:
    # Lazy so an 8GB logical array does not allocate.
    return xr.Dataset({"WSPD": ("TIME", da.zeros((n_time,), dtype="float64"))})


@pytest.fixture
def eight_gb_task(monkeypatch):
    """8GB Fargate task already holding 3GB RSS."""
    vm = MagicMock()
    vm.total = 8 * 1024**3
    vm.available = 5 * 1024**3
    monkeypatch.setattr(zarr_chunking.psutil, "virtual_memory", lambda: vm)

    mem = MagicMock()
    mem.rss = 3 * 1024**3
    proc = MagicMock()
    proc.memory_info.return_value = mem
    monkeypatch.setattr(zarr_chunking.psutil, "Process", lambda *a, **k: proc)


def test_eight_gb_task_uses_remaining_room_under_the_ceiling(eight_gb_task):
    # 8GB machine, 3GB already resident. The ceiling comes from
    # tests/config/config-test.yaml (max_chunk_gb), not the base config.
    rss = 3 * 1024**3
    ds = _dataset(1024**3)  # 8GB at float64
    steps = get_time_steps_per_chunk(ds, "TIME", MagicMock())

    assert steps * 8 <= _safe_block_bytes(8 * 1024**3, rss) + 8


def test_chunk_fits_in_the_room_under_the_peak_ceiling(monkeypatch):
    """On a large host the ceiling is max_chunk_gb from tests/config/config-test.yaml.
    RSS is already 3GB, so the block is half of what remains under that ceiling."""
    vm = MagicMock()
    vm.total = 128 * 1024**3
    monkeypatch.setattr(zarr_chunking.psutil, "virtual_memory", lambda: vm)
    mem = MagicMock()
    mem.rss = 3 * 1024**3
    proc = MagicMock()
    proc.memory_info.return_value = mem
    monkeypatch.setattr(zarr_chunking.psutil, "Process", lambda *a, **k: proc)

    rss = 3 * 1024**3
    n_time = (20 * 1024**3) // 8  # 20GB of float64
    steps = get_time_steps_per_chunk(_dataset(n_time), "TIME", MagicMock())

    assert steps * 8 <= _safe_block_bytes(128 * 1024**3, rss) + 8


def test_target_peak_uses_tighter_of_headroom_and_fraction(eight_gb_task):
    cfg = zarr_chunking._chunking_config()
    total = 8 * 1024**3
    expected = min(total - cfg.headroom_bytes, int(total * cfg.target_peak_fraction))
    assert zarr_chunking._target_peak_bytes(cfg) == expected
