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

# output_bytes = uncompressed_bytes * ratio, per format.
COMPRESSION_RATIO_NETCDF: float = 0.4
COMPRESSION_RATIO_CSV_GZIP: float = 0.15
OUTPUT_FORMAT_COMPRESSION_RATIO: dict[str, float] = {
    "netcdf": COMPRESSION_RATIO_NETCDF,
    "csv": COMPRESSION_RATIO_CSV_GZIP,
}

# GeoTIFF isn't a flat ratio: it's one .tif per (gridded var * time step), zipped.
# We sum raw raster bytes then apply GEOTIFF_ZIP_RATIO
GEOTIFF_ZIP_RATIO: float = 0.5
GEOTIFF_INT_PIXEL_BYTES: int = 4
# COMPRESSION_RATIO_GEOTIFF is only the fallback used
# when a dataset has no gridded variables.
COMPRESSION_RATIO_GEOTIFF: float = 0.5
