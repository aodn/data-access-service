"""Write one data variable to an existing NetCDF file in time blocks.

One ``Dataset.to_netcdf`` builds a Dask graph for the whole variable. A long
time series is hundreds of thousands of tasks, and that graph stays resident
while the write runs. Writing a block at a time drops the graph after each block.
"""

import gc
import math
from pathlib import Path

import numpy as np
import xarray
from netCDF4 import Dataset

from data_access_service.utils.process_logger import ProcessLogger


def append_variable(
    dataset: xarray.Dataset,
    path: Path,
    var_name: str,
    time_dim: str,
    time_per_chunk: int,
    encoding: dict,
    log,
) -> None:
    """Append ``var_name`` to ``path``.

    Numeric variables that run along ``time_dim`` are written in blocks of
    ``time_per_chunk``. Anything else is one ``to_netcdf`` append, as before.
    """
    da = dataset[var_name]
    if (
        time_dim in da.dims
        and da.dtype.kind in {"i", "u", "f"}
        and da.sizes[time_dim] > time_per_chunk
    ):
        _write_in_time_blocks(
            da, path, var_name, time_dim, time_per_chunk, encoding, log
        )
        return

    single_var_ds = xarray.Dataset({var_name: da})
    with ProcessLogger(logger=log, task_name=f"Appending variable '{var_name}'"):
        single_var_ds.to_netcdf(
            path,
            mode="a",
            engine="netcdf4",
            format="NETCDF4",
            encoding=encoding,
            compute=True,
        )
    del single_var_ds
    gc.collect()


def _write_in_time_blocks(
    da: xarray.DataArray,
    path: Path,
    var_name: str,
    time_dim: str,
    time_per_chunk: int,
    encoding: dict,
    log,
) -> None:
    n_time = da.sizes[time_dim]
    step = max(1, time_per_chunk)
    chunks = _chunksizes(da, time_dim, step)
    options = encoding.get(var_name, {})
    log.info(
        "Streaming '%s' in blocks of %d time step(s) (%d total)",
        var_name,
        step,
        n_time,
    )

    with Dataset(path, "a") as nc:
        missing = [dim for dim in da.dims if dim not in nc.dimensions]
        if missing:
            raise ValueError(
                f"Cannot stream '{var_name}': NetCDF file has no dimension(s) {missing}"
            )
        variable = nc.createVariable(
            var_name,
            da.dtype,
            tuple(da.dims),
            zlib=bool(options.get("zlib", False)),
            complevel=int(options.get("complevel", 4)),
            chunksizes=chunks,
        )
        _set_attrs(variable, da.attrs)
        # A cache the size of the block keeps a second copy of every block
        # (numpy array + cache), which is what pushed a 6GB block to ~11GB.
        variable.set_var_chunk_cache(size=1024**2, nelems=1009, preemption=1.0)

        for start in range(0, n_time, step):
            end = min(start + step, n_time)
            with ProcessLogger(
                logger=log,
                task_name=(f"Appending '{var_name}' time {start}:{end} of {n_time}"),
            ):
                block = da.isel({time_dim: slice(start, end)}).compute()
            _put_block(variable, da.dims, time_dim, start, end, block.values)
            nc.sync()
            del block
            gc.collect()


# zlib allocates a buffer the size of one NetCDF chunk. Keep that buffer small
# so it is not a second copy of the whole time block.
_NETCDF_CHUNK_BYTES = 8 * 1024 * 1024


def _chunksizes(da: xarray.DataArray, time_dim: str, step: int) -> tuple[int, ...]:
    """Chunk shape for the file. Time is at most one write block; the chunk
    itself stays within ``_NETCDF_CHUNK_BYTES``."""
    sizes = []
    for dim in da.dims:
        size = int(da.sizes[dim])
        sizes.append(min(step, size) if dim == time_dim else max(size, 1))
    itemsize = max(int(da.dtype.itemsize), 1)
    while math.prod(sizes) * itemsize > _NETCDF_CHUNK_BYTES and any(
        size > 1 for size in sizes
    ):
        for index in range(len(sizes) - 1, -1, -1):
            if sizes[index] > 1:
                sizes[index] = max(1, sizes[index] // 2)
                break
    return tuple(sizes)


def _put_block(
    variable, dims, time_dim: str, start: int, end: int, values: np.ndarray
) -> None:
    index = tuple(slice(start, end) if dim == time_dim else slice(None) for dim in dims)
    variable[index] = values


def _set_attrs(variable, attrs: dict) -> None:
    clean = {}
    for key, value in attrs.items():
        if key.startswith("_") or value is None:
            continue
        if isinstance(value, np.generic):
            value = value.item()
        elif isinstance(value, np.ndarray):
            value = value.tolist()
        clean[str(key)] = value
    if clean:
        variable.setncatts(clean)
