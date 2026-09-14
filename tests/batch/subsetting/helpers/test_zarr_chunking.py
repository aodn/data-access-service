"""Time-chunk sizing for the zarr subset write."""

from unittest.mock import MagicMock

import dask.array as da
import numpy as np
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


def test_eight_gb_task_uses_remaining_room_under_six_gb(eight_gb_task):
    # 8GB float64 along TIME. Old formula used available*0.1 = 0.5GB/chunk
    # → 16 chunks. New formula uses (6GB-3GB)*0.1 = 0.3GB/chunk → more chunks,
    # fewer time steps each, so peak stays nearer 6GB.
    ds = _dataset(1024**3)  # 8GB at float64
    steps = get_time_steps_per_chunk(ds, "TIME", MagicMock())

    old_budget = 5 * 1024**3 * 0.1
    old_steps = int(np.ceil(ds.sizes["TIME"] / np.ceil((8 * 1024**3) / old_budget)))
    assert steps < old_steps


def test_target_peak_uses_tighter_of_headroom_and_fraction(eight_gb_task):
    cfg = zarr_chunking._chunking_config()
    total = 8 * 1024**3
    expected = min(total - cfg.headroom_bytes, int(total * cfg.target_peak_fraction))
    assert zarr_chunking._target_peak_bytes(cfg) == expected
