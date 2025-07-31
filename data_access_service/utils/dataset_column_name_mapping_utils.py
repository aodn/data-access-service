import dask.dataframe


def get_date_from_parquet_dask_dataframe(
        ddf: dask.dataframe.DataFrame
):
    """
    The key for getting date in different datasets can be various.
    The various keys can be found in aodn_cloud_optimised/lib/DataQuery.py.create_time_filter()
    """
    key = "TIME"
    if key in ddf.columns:
        return ddf[key]

    key = "JULD"
    if key in ddf.columns:
        return ddf[key]

    key = "detection_timestamp"
    if key in ddf.columns:
        return ddf[key]

    raise KeyError("unknown key when getting date. Please check available columns and add new key")


