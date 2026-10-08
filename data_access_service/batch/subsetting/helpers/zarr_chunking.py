"""Size the work to the machine: thread count and time steps per dask chunk."""

import math
import os

import psutil
import xarray

from data_access_service.config.config import Config
from data_access_service.models.zarr_chunking_types import ZarrChunkingConfig


def _storage_chunk_shape(var_data: xarray.DataArray) -> tuple[int, ...] | None:
    """Zarr chunk shape stored on the variable, or None when it is not known."""
    chunks = var_data.encoding.get("chunks")
    if (
        isinstance(chunks, (tuple, list))
        and len(chunks) == var_data.ndim
        and all(isinstance(size, int) and size > 0 for size in chunks)
    ):
        return tuple(chunks)
    return None


def _bytes_read_for_one_time_step(var_data: xarray.DataArray, time_dim: str) -> int:
    """Bytes decoded to materialise one time step of ``var_data``.

    After a spatial isel, ``var_data.size`` is the polygon. Each remaining
    dask chunk is still backed by one full stored chunk, so the read is
    stored-chunk bytes times how many chunks the slice touches.
    """
    itemsize = var_data.dtype.itemsize
    n_time = var_data.sizes[time_dim]
    if n_time <= 0 or itemsize <= 0:
        return 0
    native = _storage_chunk_shape(var_data)
    chunk_layout = var_data.chunks
    if native is None or not chunk_layout:
        return math.ceil(var_data.size * itemsize / n_time)

    axis = var_data.dims.index(time_dim)
    chunks_touched = 1
    for index, parts in enumerate(chunk_layout):
        if index == axis:
            continue
        chunks_touched *= max(1, len(parts))
    stored_elems = 1
    for size in native:
        stored_elems *= size
    return chunks_touched * stored_elems * itemsize


def get_available_thread_count() -> int:
    # There is no need to use multiple threads in this context, the running machine has 8G,
    # more than 1 will blow up memory, plus interlocks make zarr read less effective.
    return 1


def _chunking_config() -> ZarrChunkingConfig:
    return Config.get_config().get_zarr_chunking_config()


def _target_peak_bytes(cfg: ZarrChunkingConfig) -> int:
    total = psutil.virtual_memory().total
    return min(total - cfg.headroom_bytes, int(total * cfg.target_peak_fraction))


def get_time_steps_per_chunk(
    dataset: xarray.Dataset,
    time_dim: str,
    log,
) -> int:
    """Time steps that fit in the room left under the RSS ceiling.

    The ceiling is the lower of the host peak and ``max_chunk_gb``. Memory
    already resident is subtracted first, and the remainder is halved because
    a block is briefly held twice while it is materialised.
    """
    cfg = _chunking_config()
    vm = psutil.virtual_memory()
    current_rss = psutil.Process(os.getpid()).memory_info().rss
    target_peak = _target_peak_bytes(cfg)
    # max_chunk_gb is a hard ceiling on process RSS, not the size of the
    # array. Room left under that ceiling is all a new block may use, and the
    # block is loaded twice for a moment (dask result, then the numpy array).
    ceiling = min(target_peak, cfg.max_chunk_bytes)
    room = max(0, ceiling - current_rss)
    safe_memory_per_thread = max(cfg.min_chunk_bytes, room // 2)
    log.info("total memory in MB: %d", vm.total / (1024 * 1024))
    log.info(
        "Peak ceiling: %.2f GB, current RSS: %.2f GB, "
        "room: %.2f GB, chunk size: %d MB",
        ceiling / (1024**3),
        current_rss / (1024**3),
        room / (1024**3),
        safe_memory_per_thread / (1024**2),
    )

    # A cropped view reports only the cells inside the polygon. Reading one
    # day still decodes every stored chunk that view touches. GHRSST L3S over
    # the full time range and a small box (this job: all dates, ~120-127E,
    # 10-14S) is a few MB on screen and a full spatial chunk on read.
    estimated_size = 0
    bytes_per_step = 0
    for var_data in dataset.data_vars.values():
        if not hasattr(var_data, "dtype") or not hasattr(var_data, "size"):
            continue
        estimated_size += var_data.size * var_data.dtype.itemsize
        if time_dim in getattr(var_data, "dims", ()):
            bytes_per_step += _bytes_read_for_one_time_step(var_data, time_dim)

    # Fallback: use conservative estimate based on dimensions
    if estimated_size == 0:
        total_elements = 1
        for dim_size in dataset.sizes.values():
            total_elements *= dim_size
        # Assume average 8 bytes per element (float64)
        estimated_size = total_elements * 8

    log.info(f"Estimated dataset size: {estimated_size / (1024**3):.2f} GB")
    total_time_count = dataset.sizes[time_dim]
    if total_time_count <= 1 or estimated_size <= 0:
        log.info("Chunk count: 1")
        return 1

    if bytes_per_step <= 0:
        bytes_per_step = math.ceil(estimated_size / total_time_count)
    # Floor, do not ceil. ceil(time / ceil(size / budget)) jumps to 2 steps
    # whenever a step is just over half the budget, and that block is then
    # almost twice the budget.
    steps = min(total_time_count, max(1, safe_memory_per_thread // bytes_per_step))
    log.info("Chunk count: %d", math.ceil(total_time_count / steps))
    if bytes_per_step > safe_memory_per_thread:
        log.info(
            "One time step is %d MB, over the %d MB block budget",
            bytes_per_step // (1024 * 1024),
            safe_memory_per_thread // (1024 * 1024),
        )
    return steps
