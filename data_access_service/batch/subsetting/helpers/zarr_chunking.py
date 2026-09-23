"""Size the work to the machine: thread count and time steps per dask chunk."""

import math
import os

import psutil
import xarray

from data_access_service.config.config import Config
from data_access_service.models.zarr_chunking_types import ZarrChunkingConfig


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
    """
    Calculate the number of time steps per chunk based on available memory and dataset size.
    This helps to optimize memory usage during processing.
    memory_fraction (config) is the fraction of the peak budget to use for one
    chunk. The value is only for safety (dask/zlib copies).
    """
    cfg = _chunking_config()
    vm = psutil.virtual_memory()
    current_rss = psutil.Process(os.getpid()).memory_info().rss
    target_peak = _target_peak_bytes(cfg)
    # Remaining room under the peak cap, not "whatever the OS says is free",
    # so an 8GB task with 3GB already used does not pick a chunk that lands
    # at 6.7GB.
    budget = max(cfg.min_chunk_bytes, target_peak - current_rss)
    log.info("total memory in MB: %d", vm.total / (1024 * 1024))
    log.info(
        "Target peak: %.2f GB, current RSS: %.2f GB, remaining budget: %.2f GB",
        target_peak / (1024**3),
        current_rss / (1024**3),
        budget / (1024**3),
    )
    safe_memory_per_thread = int(
        budget * cfg.memory_fraction / get_available_thread_count()
    )
    safe_memory_per_thread = max(cfg.min_chunk_bytes, safe_memory_per_thread)
    log.info("Chunk size: %d MB per thread", safe_memory_per_thread / (1024**2))

    # var.nbytes forces computation - use size * itemsize instead
    estimated_size = 0
    for var_name, var_data in dataset.data_vars.items():
        if hasattr(var_data, "dtype") and hasattr(var_data, "size"):
            estimated_size += var_data.size * var_data.dtype.itemsize

    # Fallback: use conservative estimate based on dimensions
    if estimated_size == 0:
        total_elements = 1
        for dim_size in dataset.sizes.values():
            total_elements *= dim_size
        # Assume average 8 bytes per element (float64)
        estimated_size = total_elements * 8

    log.info(f"Estimated dataset size: {estimated_size / (1024**3):.2f} GB")
    chunk_count = max(1, math.ceil(estimated_size / safe_memory_per_thread))
    log.info("Chunk count: %d", chunk_count)
    total_time_count = dataset.sizes[time_dim]
    return math.ceil(total_time_count / chunk_count)
