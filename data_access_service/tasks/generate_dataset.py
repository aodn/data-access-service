import json
from typing import List, Dict, Optional

import dask.dataframe as ddf
import pandas as pd
import xarray
from numcodecs import Zlib

from data_access_service import API, init_log, Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.constants import PARTITION_KEY
from data_access_service.core.descriptor import Descriptor
from data_access_service.tasks.data_file_upload import (
    upload_all_files_in_folder_to_temp_s3,
)
from data_access_service.utils.date_time_utils import (
    get_monthly_utc_date_range_array_from_,
    trim_date_range,
    check_rows_with_date_range,
)

efs_mount_point = "/mount/efs/"

config: Config = Config.get_config()
log = init_log(config)


def process_data_files(
    api: API,
    job_id_of_init: str,
    job_index: str,
    intermediate_output_folder: str,
    uuid: str,
    keys: List[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    multi_polygon: str | None,
) -> str | None:
    if multi_polygon is not None:
        multi_polygon_dict = json.loads(multi_polygon)
    else:
        multi_polygon_dict = None

    if None in [uuid, keys, start_date, end_date, intermediate_output_folder]:
        raise ValueError("One or more required arguments are None")

    if "*" in keys:
        # We need to expand to include all filename as key give "*" as wildcard
        md: Dict[str, Descriptor] = api.get_mapped_meta_data(uuid)
        # key are all file name associated given UUID
        dataset = md.keys()
    else:
        dataset = keys

    aws = AWSHelper()

    for datum in dataset:
        try:
            log.info(f"Start prepare {uuid}-{datum}")
            has_result = _generate_partition_output_with_polygon(
                api,
                intermediate_output_folder,
                job_index,
                uuid,
                datum,
                start_date,
                end_date,
                multi_polygon_dict,
            )
            if has_result:
                upload_all_files_in_folder_to_temp_s3(
                    master_job_id=job_id_of_init,
                    local_folder=intermediate_output_folder,
                    aws=aws,
                )
        except TypeError as e:
            log.error(f"Error: {e}")
            raise e
        except ValueError as e:
            log.error(f"Error: {e}")
            raise e
        except KeyError as e:
            # We do not throw the error again to avoid blocking other datum from processing
            # in the for loop
            log.error(f"{e}, likely due to malform source file {datum}")
        except MemoryError as e:
            # If you try to convert a zarr to CSV, it will be too big to fit into memory or file due to multiple dimension
            # hence it is not something we can support
            raise MemoryError(f"Data file {datum} too big to convert subset : {e}")
        except Exception as e:
            log.error(f"Error: {e}")
    return None


def _generate_partition_output(
    api: API,
    root_folder_path: str,
    job_index: str,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
):
    has_data = False
    # We need to split it smaller due to fact that the lib return data with to_parquet internally
    # which use a lot of memory.
    start_date, end_date = trim_date_range(
        api=api,
        uuid=uuid,
        key=key,
        requested_start_date=start_date,
        requested_end_date=end_date,
    )

    if start_date is not None and end_date is not None:
        date_ranges = get_monthly_utc_date_range_array_from_(
            start_date=start_date, end_date=end_date
        )
        datasource = api.get_datasource(uuid, key)
        checked_date_ranges = check_rows_with_date_range(datasource, date_ranges)

        need_append = False
        for date_range in checked_date_ranges:
            result: Optional[ddf.DataFrame | xarray.Dataset] = query_data(
                api,
                uuid,
                key,
                date_range["start_date"],
                date_range["end_date"],
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )
            if result is not None:
                if key.endswith("parquet"):
                    # With parquet we can write on each result because of the partition by TIME
                    # create different directory
                    output_path = f"{root_folder_path}/{key}/part-{job_index}/"

                    # Derive partition key without time
                    time_key = api.map_column_names(
                        uuid=uuid, key=key, columns=["TIME"]
                    )[0]
                    result[PARTITION_KEY] = result[time_key].dt.strftime("%Y-%m")

                    result.to_parquet(
                        output_path,
                        partition_on=[PARTITION_KEY],  # Partition by region column
                        compression="zstd",  # Use Zstd for small file size
                        engine="pyarrow",  # Use pyarrow for performance
                        write_index=False,  # Exclude index to save space
                    )
                else:
                    # Zarr do not support directory partition hence we need to consolidate
                    # it before write to disk.
                    output_path = f"{root_folder_path}/{key}/part-{job_index}.zarr"
                    if not need_append:
                        # Get all data variable names
                        variables = list(result.data_vars)
                        encoding = {
                            # Must use Zlib for now as netcdf do not support other compression
                            var: {"compressor": Zlib(level=9)}
                            for var in variables
                        }
                        result.to_zarr(
                            output_path, mode="w", encoding=encoding, compute=True
                        )
                        need_append = True
                    else:
                        time_dim = api.map_column_names(
                            uuid=uuid, key=key, columns=["TIME"]
                        )[0]
                        result.to_zarr(
                            output_path, mode="a", append_dim=time_dim, compute=True
                        )

                # Either parquet or zarr save correct and no exception
                has_data = True
            else:
                log.info(
                    f"No data found for uuid={uuid}, key={key}, date_range={date_range}"
                )

    return has_data


def _generate_partition_output_with_polygon(
    api: API,
    folder_path: str,
    array_index: str,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    multi_polygon: dict | None,
) -> bool:

    had_data = False
    if multi_polygon is not None:
        # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
        #  we can change to use the polygon coordinates directly
        for polygon in multi_polygon["coordinates"]:
            lats_lons = get_lat_lon_from_(polygon)
            min_lat = lats_lons["min_lat"]
            max_lat = lats_lons["max_lat"]
            min_lon = lats_lons["min_lon"]
            max_lon = lats_lons["max_lon"]

            had_data = had_data or _generate_partition_output(
                api,
                folder_path,
                array_index,
                uuid,
                key,
                start_date,
                end_date,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )
    else:
        had_data = _generate_partition_output(
            api,
            folder_path,
            array_index,
            uuid,
            key,
            start_date,
            end_date,
            None,
            None,
            None,
            None,
        )

    if not had_data:
        log.info(
            f" No data found for uuid={uuid}, start_date={start_date}, end_date={end_date}, multi_polygon={multi_polygon}"
        )
        return False
    else:
        return True


def query_data(
    api,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
) -> Optional[ddf.DataFrame | xarray.Dataset]:
    log.info(
        f"Querying data for uuid={uuid}, key={key}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )

    try:
        df: Optional[ddf.DataFrame | xarray.Dataset] = api.get_dataset(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
        )
        if df is not None:
            return df
        else:
            log.info("No data found for the given parameters")
            return None
    except ValueError as e:
        log.info(f"seems like no data for this polygon. Error: {e}")

        # sometimes even though we get the temoral extents correctly, the requested date range may still be out of bounds because we want to cover nanoseconds precision.
        # e.g. ValueError: date_start=2021-02-01 00:00:00.000000000 is out of range of dataset. The maximum date_end is 2021-02-01 00:00:00.
        # so we need to check the error message and ignore it if the two dates are close.
        # In summary, this error is not that important so it needs to be reduced the weight, from throwing it to logging it.
        if "is out of range of dataset" in str(e):
            log.error(
                f"The provided date range is out of bounds for the dataset. Error message is: `{e}`. Please check whether it is acceptable."
            )
            return None

        raise e
    except Exception as e:
        log.error(f"Error: {e}")
        raise e


def get_lat_lon_from_(polygon: List[List[List[float]]]) -> Dict[str, float]:
    coordinates = [coord for ring in polygon for coord in ring]
    lats = [coord[1] for coord in coordinates]
    lons = [coord[0] for coord in coordinates]

    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }


def generate_zip_name(uuid, start_date, end_date):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    return f"{uuid}_{start_date_str}_{end_date_str}"
