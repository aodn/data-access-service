"""Time-chunk sizing for the zarr subset write."""

from unittest.mock import MagicMock

import dask.array as da
import pytest
import xarray as xr

from data_access_service.batch.subsetting.helpers import zarr_chunking
from data_access_service.batch.subsetting.helpers.zarr_chunking import (
    get_time_steps_per_chunk,
)


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
    # 8GB machine, 3GB already resident, ceiling 4GB. One block may use half
    # of the 1GB still free.
    ds = _dataset(1024**3)  # 8GB at float64
    steps = get_time_steps_per_chunk(ds, "TIME", MagicMock())

    assert steps * 8 <= (1 * 1024**3) // 2


def test_chunk_fits_in_the_room_under_the_peak_ceiling(monkeypatch):
    """RSS is already 3GB and the ceiling is 4GB. The block has to fit in the
    1GB left, halved because the block is briefly in memory twice — not be a
    2GB array added on top of the 3GB already resident."""
    vm = MagicMock()
    vm.total = 128 * 1024**3
    monkeypatch.setattr(zarr_chunking.psutil, "virtual_memory", lambda: vm)
    mem = MagicMock()
    mem.rss = 3 * 1024**3
    proc = MagicMock()
    proc.memory_info.return_value = mem
    monkeypatch.setattr(zarr_chunking.psutil, "Process", lambda *a, **k: proc)

    n_time = (20 * 1024**3) // 8  # 20GB of float64
    steps = get_time_steps_per_chunk(_dataset(n_time), "TIME", MagicMock())

    assert steps * 8 <= (1 * 1024**3) // 2


def test_target_peak_uses_tighter_of_headroom_and_fraction(eight_gb_task):
    cfg = zarr_chunking._chunking_config()
    total = 8 * 1024**3
    expected = min(total - cfg.headroom_bytes, int(total * cfg.target_peak_fraction))
    assert zarr_chunking._target_peak_bytes(cfg) == expected
