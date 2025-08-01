import dask.dataframe
import xarray


def get_time_key_from_parquet(ddf: dask.dataframe.DataFrame):
    """
    The key for getting date in different parquet datasets can be various.
    The various keys and checking orders can be found in aodn_cloud_optimised/lib/DataQuery.py.create_time_filter()
    """
    if "TIME" in ddf.columns:
        return "TIME"

    if "JULD" in ddf.columns:
        return "JULD"

    if "detection_timestamp" in ddf.columns:
        return "detection_timestamp"

    raise KeyError(
        "unknown key when getting date. Please check available columns and add new key"
    )


def get_time_key_from_zarr(xarray_dataset: xarray.Dataset):
    """
    The key for getting time dimension in different datasets can be various.

    This function returns the key, not value
    """
    if "time" in xarray_dataset.dims:
        return "time"

    if "TIME" in xarray_dataset.dims:
        return "TIME"

    raise KeyError(
        "unknown key when getting time dimension. Please check available dimensions and add new key"
    )
