import sys

import pandas as pd

from data_access_service.models.bounding_box import BoundingBox

# Marker value for an optional field the user did not provide (dates,
# multi_polygon, ...). AWS Batch job parameters are plain strings and cannot
# carry None, so absent values travel as this literal instead.
NON_SPECIFIED = "non-specified"

# The Unix epoch; earliest timestamp the service works with
UNIX_EPOCH_UTC: pd.Timestamp = pd.Timestamp("1970-01-01 00:00:00.000000000", tz="UTC")

WHOLE_GLOBE_BBOX = BoundingBox(min_lon=-180, min_lat=-90, max_lon=180, max_lat=90)

COORDINATE_INDEX_PRECISION = 1
DEPTH_INDEX_PRECISION = -1
RECORD_PER_PARTITION: int = 1000

STR_TIME_UPPER_CASE = sys.intern("TIME")
STR_TIME_LOWER_CASE = sys.intern("time")
STR_LONGITUDE_LOWER_CASE = sys.intern("longitude")
STR_LATITUDE_LOWER_CASE = sys.intern("latitude")
STR_DEPTH_LOWER_CASE = sys.intern("depth")
STR_LATITUDE_UPPER_CASE = sys.intern("LATITUDE")
STR_LONGITUDE_UPPER_CASE = sys.intern("LONGITUDE")

STATUS = "status"
MESSAGE = "message"
DATA = "data"
PARTITION_KEY = sys.intern("PARTITION_KEY")

# aodn_cloud_optimised adds the hive partition columns to a dataset's schema
# alongside the real columns, so name alone cannot tell them apart. The only
# marker surviving into the catalog is long_name; the declared type is not
# usable (_common_metadata says int64, pyarrow infers int32 from the directory).
PARTITION_KEY_LONG_NAMES = frozenset(
    {"Partition timestamp", "Spatial partition polygon"}
)

# Names the cloud-optimised pipeline always uses for its hive partitions. Older
# datasets predate the long_name marker above, so the name is the only clue left.
# Matched by name ONLY when the declared type is not temporal, so a dataset that
# genuinely has a "timestamp" column of type timestamp[ns] is still usable.
PARTITION_KEY_NAMES = frozenset({"timestamp", "polygon"})

PARQUET_SUBSET_ROW_NUMBER: int = 200000
MAX_PARQUET_SPLIT: int = 30
MAX_CSV_ROW: int = 1048576

COMPRESSION_RATIO_NETCDF: float = 0.2
COMPRESSION_RATIO_CSV_GZIP: float = 0.15
OUTPUT_FORMAT_COMPRESSION_RATIO: dict[str, float] = {
    "netcdf": COMPRESSION_RATIO_NETCDF,
    "csv": COMPRESSION_RATIO_CSV_GZIP,
}

ASSUMED_STRING_BYTES: int = 64

SSE_WORKER_THREADS: int = 16

GEOTIFF_ZIP_RATIO: float = 0.2
GEOTIFF_INT_PIXEL_BYTES: int = 4
GEOTIFF_CURVILINEAR_INFLATION: float = 1.5
