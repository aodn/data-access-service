import json
import os
import geojson
import dask.dataframe as ddf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from shapely.geometry import Polygon as ShapelyPolygon

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource
from geojson import MultiPolygon
from typing import Dict, Optional

from data_access_service.core.constants import STR_LONGITUDE_UPPER_CASE
from data_access_service.core.constants import STR_LATITUDE_UPPER_CASE
from data_access_service import API, init_log, Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.constants import PARTITION_KEY
from data_access_service.core.descriptor import Descriptor
from data_access_service.models.subset_request import NON_SPECIFIED, SubsetRequest
from data_access_service.batch.subsetting.helpers.data_file_upload import (
    upload_all_files_in_folder_to_temp_s3,
)
from data_access_service.batch.subsetting.helpers.parquet_polygon_ranges import (
    POLYGON_PARTITION,
    PolygonRange,
    list_polygon_values,
    select_polygons_in_range,
)
from data_access_service.batch.subsetting.helpers.parquet_date_ranges import (
    check_rows_with_date_range,
    trim_date_range,
)
from data_access_service.utils.date_time_utils import (
    get_monthly_utc_date_range_array_from_,
)
from data_access_service.utils.multi_polygon_helper import merge_polygons
from pandas._libs import NaTType

efs_mount_point = "/mount/efs/"

config: Config = Config.get_config()
log = init_log(config)


def process_parquet_files(
    api: API,
    job_id_of_init: str,
    job_index: str,
    intermediate_output_folder: str,
    subset_request: SubsetRequest,
    start_date: pd.Timestamp | NaTType,
    end_date: pd.Timestamp | NaTType,
    polygon_range: Optional[PolygonRange] = None,
) -> str | None:
    """Prepare the parquet output of one child job.

    :param polygon_range: set for a dataset without a time column; the child
        then reads the `polygon` partitions in `[lo, hi)` instead of date windows
    """
    uuid = subset_request.uuid
    keys = subset_request.keys
    multi_polygon = subset_request.multi_polygon

    if multi_polygon is not None:
        if multi_polygon == NON_SPECIFIED:
            #    multi polygon dict is whole earth
            multi_polygon = '{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}'
        multi_polygon_dict = geojson.loads(multi_polygon)
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
                polygon_range,
            )
            if has_result:
                return upload_all_files_in_folder_to_temp_s3(
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
            # A parquet dataset too large to fit into memory or a single file
            # is not something we can support
            raise MemoryError(f"Data file {datum} too big to convert subset : {e}")
        except Exception as e:
            # Never swallow: on an unexpected failure this datum writes no output,
            # the collector finds nothing and emails "No data available" as if the
            # dataset were empty (issue 9144). Fail loudly so the error is visible.
            log.error(f"Error: {e}")
            raise
    return None


def _is_empty_window_error(error: ValueError) -> bool:
    """True when the query missed the dataset and this window can be skipped.

    Same two cases `query_data` used to swallow: a date that only misses because
    of nanosecond rounding, and a bbox that does not meet the dataset extent.
    """
    message = str(error)
    if "is out of range of dataset" in message:
        log.error(
            "The provided date range is out of bounds for the dataset. "
            f"Error message is: `{error}`."
        )
        return True
    if "No data for given bounding box. Amend lat/lon values" in message:
        log.error(
            "The provided bounding box does not intersect with the dataset's "
            f"spatial extent. Error message is: `{error}`."
        )
        return True
    return False


def _next_part_path(directory: Path) -> Path:
    """Next `part.N.parquet` in `directory` so a later window does not overwrite."""
    index = 0
    while True:
        candidate = directory / f"part.{index}.parquet"
        if not candidate.exists():
            return candidate
        index += 1


def _write_batches(
    batches,
    output_dir: str,
    partition_label: Optional[str],
    time_key: Optional[str],
    polygon: Optional[ShapelyPolygon],
    lat_key: Optional[str],
    lon_key: Optional[str],
) -> bool:
    """Write each batch to its hive partition and drop it before the next.

    `partition_label` pins every row (polygon partitions have no time column).
    Otherwise the month comes from `time_key`, because a window that starts on
    the last day of a month also contains the next month.
    """
    writers: dict[str, pq.ParquetWriter] = {}
    schema = None
    wrote = False
    try:
        for batch in batches:
            if batch.num_rows == 0:
                continue
            batch_schema = schema if schema is not None else batch.schema
            frame = batch.to_pandas()
            del batch
            if polygon is not None and lat_key is not None and lon_key is not None:
                frame = _filter_partition_by_polygon(frame, polygon, lat_key, lon_key)
            if frame.empty:
                del frame
                continue

            if partition_label is not None:
                groups = [(partition_label, frame)]
            else:
                series = frame[time_key]
                if series.dtype.kind != "M":
                    series = pd.to_datetime(series)
                labelled = frame.copy()
                labelled["_part"] = series.dt.strftime("%Y-%m")
                groups = [
                    (label, part.drop(columns="_part"))
                    for label, part in labelled.groupby("_part", sort=False)
                ]
                del labelled

            for label, part in groups:
                if part.empty:
                    continue
                table = pa.Table.from_pandas(
                    part, schema=batch_schema, preserve_index=False
                )
                writer = writers.get(label)
                if writer is None:
                    part_dir = Path(output_dir) / f"{PARTITION_KEY}={label}"
                    part_dir.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(
                        _next_part_path(part_dir), table.schema, compression="zstd"
                    )
                    writers[label] = writer
                    if schema is None:
                        schema = table.schema
                writer.write_table(table)
                wrote = True
                del part
                del table
            del frame
            pa.default_memory_pool().release_unused()
    finally:
        for writer in writers.values():
            writer.close()
    return wrote


def _stream_window_to_parquet(
    api: API,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
    output_path: str,
    partition_label: Optional[str] = None,
    time_key: Optional[str] = None,
    polygon: Optional[ShapelyPolygon] = None,
    lat_key: Optional[str] = None,
    lon_key: Optional[str] = None,
    scalar_filter: Optional[dict] = None,
) -> bool:
    """Scan one window in row batches and write them. False when it has no rows."""
    log.info(
        f"Querying data for uuid={uuid}, key={key}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )
    try:
        batches = api.iter_parquet_batches(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
            scalar_filter=scalar_filter,
        )
        return _write_batches(
            batches,
            output_path,
            partition_label,
            time_key,
            polygon,
            lat_key,
            lon_key,
        )
    except ValueError as error:
        if _is_empty_window_error(error):
            return False
        raise


def _filter_partition_by_polygon(df, shapely_poly, lat_key, lon_key):
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_key], df[lat_key]),
    )
    # covered_by, not within: within drops points on the edge, e.g. a site at
    # longitude -180 when the polygon is the whole earth
    gdf_filtered = gdf[gdf.covered_by(shapely_poly)]
    return gdf_filtered.drop(columns=["geometry"])


def _generate_partition_output(
    api: API,
    root_folder_path: str,
    job_index: str,
    uuid: str,
    key: str,
    start_date: pd.Timestamp | NaTType,
    end_date: pd.Timestamp | NaTType,
    polygon: Optional[ShapelyPolygon] = None,
):
    has_data = False
    # One calendar month at a time. Each month is scanned in row batches so the
    # window is never one pandas frame (that frame is what pushed argo near 8 GB).
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
        # extract table schema for parquet dataset
        if datasource is not None and isinstance(datasource, ParquetDataSource):
            # save to the root_folder/dataschema.json
            schema_path = f"{root_folder_path}/dataschema.json"
            if not Path(schema_path).exists():
                table_schema = datasource.get_metadata()
                os.makedirs(os.path.dirname(schema_path), exist_ok=True)
                with open(schema_path, "w") as f:
                    json.dump(table_schema, f, indent=2)

                log.info(f"Saved table schema to {schema_path}")

            checked_date_ranges = check_rows_with_date_range(
                api, uuid, key, datasource, date_ranges
            )
            log.info(
                "Processing %s date window(s) for uuid=%s key=%s",
                len(checked_date_ranges),
                uuid,
                key,
            )

            if polygon is not None:
                min_lon, min_lat, max_lon, max_lat = polygon.bounds
            else:
                min_lat = None
                max_lat = None
                min_lon = None
                max_lon = None

            lat_key = None
            lon_key = None
            if polygon is not None:
                values = api.map_column_names(
                    uuid=uuid,
                    key=key,
                    columns=[STR_LATITUDE_UPPER_CASE, STR_LONGITUDE_UPPER_CASE],
                )
                if values is not None:
                    lat_key, lon_key = values

            if not checked_date_ranges:
                return has_data

            time_key = api.require_time_column(uuid=uuid, key=key)
            output_path = f"{root_folder_path}/{key}/part-{job_index}/"

            for index, date_range in enumerate(checked_date_ranges, start=1):
                log.info(
                    "Window %s/%s: %s → %s",
                    index,
                    len(checked_date_ranges),
                    date_range["start_date"],
                    date_range["end_date"],
                )
                log.info(
                    "Writing parquet for window %s/%s to %s",
                    index,
                    len(checked_date_ranges),
                    output_path,
                )
                wrote = _stream_window_to_parquet(
                    api,
                    uuid,
                    key,
                    date_range["start_date"],
                    date_range["end_date"],
                    min_lat,
                    max_lat,
                    min_lon,
                    max_lon,
                    output_path,
                    time_key=time_key,
                    polygon=polygon,
                    lat_key=lat_key,
                    lon_key=lon_key,
                )
                if wrote:
                    log.info(f"Saved partition to {output_path}")
                    has_data = True
                else:
                    log.info(
                        f"No data found for uuid={uuid}, key={key}, date_range={date_range}"
                    )

    return has_data


def _generate_polygon_partition_output(
    api: API,
    root_folder_path: str,
    job_index: str,
    uuid: str,
    key: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    polygon_range: PolygonRange,
    polygon: Optional[ShapelyPolygon] = None,
    shape_index: int = 0,
) -> bool:
    """Write the `polygon` partitions in `polygon_range`, one partition at a time.

    Used for a dataset without a time column, where date windows cannot split
    the work. Each polygon partition is scanned in row batches, so memory
    follows one batch rather than the whole partition.
    """
    has_data = False
    datasource = api.get_datasource(uuid, key)
    if datasource is None or not isinstance(datasource, ParquetDataSource):
        return has_data

    schema_path = f"{root_folder_path}/dataschema.json"
    if not Path(schema_path).exists():
        table_schema = datasource.get_metadata()
        os.makedirs(os.path.dirname(schema_path), exist_ok=True)
        with open(schema_path, "w") as f:
            json.dump(table_schema, f, indent=2)

        log.info(f"Saved table schema to {schema_path}")

    partition_values = select_polygons_in_range(
        list_polygon_values(datasource), polygon_range
    )
    log.info(
        "Processing %s polygon partition(s) in range %s for uuid=%s key=%s",
        len(partition_values),
        polygon_range,
        uuid,
        key,
    )

    if polygon is not None:
        min_lon, min_lat, max_lon, max_lat = polygon.bounds
    else:
        min_lat = None
        max_lat = None
        min_lon = None
        max_lon = None

    lat_key = None
    lon_key = None
    if polygon is not None:
        lat_key, lon_key = api.map_column_names(
            uuid=uuid,
            key=key,
            columns=[STR_LATITUDE_UPPER_CASE, STR_LONGITUDE_UPPER_CASE],
        )

    output_path = f"{root_folder_path}/{key}/part-{job_index}/"
    for index, partition_value in enumerate(partition_values, start=1):
        log.info("Polygon partition %s/%s", index, len(partition_values))
        # No time column to derive a month from; a label per partition (and per
        # requested shape) keeps each write in its own directory.
        wrote = _stream_window_to_parquet(
            api,
            uuid,
            key,
            start_date,
            end_date,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            output_path,
            partition_label=f"polygon-{shape_index}-{index}",
            polygon=polygon,
            lat_key=lat_key,
            lon_key=lon_key,
            scalar_filter={POLYGON_PARTITION: partition_value},
        )
        if not wrote:
            log.info(
                f"No data found for uuid={uuid}, key={key}, polygon partition {index}"
            )
            continue

        log.info(f"Saved polygon partition {index} to {output_path}")
        has_data = True

    return has_data


def _generate_partition_output_with_polygon(
    api: API,
    folder_path: str,
    array_index: str,
    uuid: str,
    key: str,
    start_date: pd.Timestamp | NaTType,
    end_date: pd.Timestamp | NaTType,
    multi_polygon: MultiPolygon | None,
    polygon_range: Optional[PolygonRange] = None,
) -> bool:

    had_data = False
    if multi_polygon is not None:
        # The multiple polygons may overlap, merge those overlaps into
        # non-overlapping polygons
        for shape_index, shapely_poly in enumerate(merge_polygons(multi_polygon)):
            if polygon_range is not None:
                polygon_had_data = _generate_polygon_partition_output(
                    api,
                    folder_path,
                    array_index,
                    uuid,
                    key,
                    start_date,
                    end_date,
                    polygon_range,
                    shapely_poly,
                    shape_index,
                )
            else:
                polygon_had_data = _generate_partition_output(
                    api,
                    folder_path,
                    array_index,
                    uuid,
                    key,
                    start_date,
                    end_date,
                    shapely_poly,
                )
            had_data = had_data or polygon_had_data
    elif polygon_range is not None:
        had_data = _generate_polygon_partition_output(
            api,
            folder_path,
            array_index,
            uuid,
            key,
            start_date,
            end_date,
            polygon_range,
            None,
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
    scalar_filter: Optional[dict] = None,
) -> Optional[ddf.DataFrame]:
    log.info(
        f"Querying data for uuid={uuid}, key={key}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )

    # Only pass scalar_filter when set, so the date-window call is unchanged
    extra_filters = {} if scalar_filter is None else {"scalar_filter": scalar_filter}
    try:
        df: Optional[ddf.DataFrame] = api.get_dataset(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
            **extra_filters,
        )
        if df is not None:
            return df
        else:
            log.info("No data found for the given parameters")
            return None
    except ValueError as e:
        log.info(f"seems like no data for this polygon. Error: {e}")
        # A date that only misses on nanosecond rounding, or a bbox that misses
        # the dataset extent, is an empty window rather than a failed job.
        if _is_empty_window_error(e):
            return None

        raise e
    except Exception as e:
        log.error(f"{type(e)}: {e}")
        raise e
