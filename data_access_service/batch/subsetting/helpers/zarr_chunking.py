"""Size the work to the machine: thread count and time steps per dask chunk."""

import math
import os

import psutil
import xarray


def get_available_thread_count(log) -> int:
    """Threads to hand dask. Dev/testing stays single-threaded so local runs and
    CI are reproducible and don't oversubscribe the box."""
    if os.getenv("PROFILE") in (None, "dev", "testing"):
        log.info("Running in dev or testing mode, using 1 thread")
        return 1

    cpu_count = psutil.cpu_count(logical=True)
    log.info(f"Available thread count: {cpu_count}")
    return cpu_count


def get_time_steps_per_chunk(
    dataset: xarray.Dataset,
    time_dim: str,
    log,
    memory_fraction: float = 0.1,
) -> int:
    """
    Calculate the number of time steps per chunk based on available memory and dataset size.
    This helps to optimize memory usage during processing.
    the memory_fraction is the fraction of available memory to use for processing. The
    value is only for safety. Can be adjusted based on the experience.
    """
    available_memory = psutil.virtual_memory().available
    log.info("total memory in MB: %d", psutil.virtual_memory().total / (1024 * 1024))
    log.info(f"Available memory in MB: {available_memory / (1024 * 1024):.2f}")
    safe_memory_per_thread = int(
        available_memory * memory_fraction / get_available_thread_count(log)
    )
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
