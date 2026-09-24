"""A long time axis is written in blocks, not as one Dask graph."""

import numpy as np
import xarray
from dask import array as da

from data_access_service.batch.subsetting.helpers.netcdf_stream import append_variable


def test_time_variable_is_written_in_blocks(tmp_path):
    n_time, n_lat, n_lon = 5, 3, 4
    values = np.arange(n_lat * n_time * n_lon, dtype=np.float32).reshape(
        n_lat, n_time, n_lon
    )
    # Many time chunks, so a single to_netcdf would be one large graph.
    # Time is not the first axis: the write index has to follow the variable.
    lazy = da.from_array(values, chunks=(n_lat, 1, n_lon))
    dataset = xarray.Dataset(
        {
            "npp": (
                ("lat", "time", "lon"),
                lazy,
                {"units": "mg C m-2 day-1"},
            ),
            "crs": ((), np.int32(4326)),
        },
        coords={
            "time": np.arange(n_time),
            "lat": np.arange(n_lat),
            "lon": np.arange(n_lon),
        },
    )
    path = tmp_path / "out.nc"
    xarray.Dataset(coords=dataset.coords).to_netcdf(path, mode="w", engine="netcdf4")

    class _Log:
        def info(self, *args, **kwargs):
            pass

    append_variable(
        dataset,
        path,
        "npp",
        time_dim="time",
        time_per_chunk=2,
        encoding={"npp": {"zlib": True, "complevel": 5}},
        log=_Log(),
    )
    append_variable(
        dataset,
        path,
        "crs",
        time_dim="time",
        time_per_chunk=2,
        encoding={},
        log=_Log(),
    )

    written = xarray.open_dataset(path)
    np.testing.assert_array_equal(written["npp"].values, values)
    assert written["npp"].attrs["units"] == "mg C m-2 day-1"
    assert int(written["crs"]) == 4326
    # Time is not the first axis. The block index must follow the variable's dims.
    assert written["npp"].dims == ("lat", "time", "lon")
