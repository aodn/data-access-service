import sys

import pandas as pd

from data_access_service.models.bounding_box import BoundingBox

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

# Bytes one value of each type occupies once written as CSV
# Integers are keyed by bit width because the spread is large (4 bytes vs 20)
# and QC-flag columns are commonly int8. Anything not listed falls back to
# ASSUMED_STRING_BYTES.
CSV_BYTES_PER_FLOAT64: int = 24
CSV_BYTES_PER_FLOAT32: int = 15
CSV_BYTES_PER_INT: dict[int, int] = {8: 4, 16: 6, 32: 11, 64: 20}
CSV_BYTES_PER_BOOL: int = 5
CSV_BYTES_PER_TIMESTAMP: int = 29
CSV_BYTES_PER_DATE: int = 10
CSV_BYTES_PER_NULL: int = 2

# One separator (or the line terminator on the last column) per column.
CSV_SEPARATOR_BYTES: int = 1

MAX_FRAGMENT_FOOTER_READS: int = 256

GEOTIFF_ZIP_RATIO: float = 0.2
GEOTIFF_INT_PIXEL_BYTES: int = 4
GEOTIFF_CURVILINEAR_INFLATION: float = 1.5
