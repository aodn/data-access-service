import sys

COORDINATE_INDEX_PRECISION = 1
DEPTH_INDEX_PRECISION = -1
RECORD_PER_PARTITION: int = 1000

STR_TIME = sys.intern("TIME")
STR_TIME_LOWER_CASE = sys.intern("time")
STR_LONGITUDE_LOWER_CASE = sys.intern("longitude")
STR_LATITUDE_LOWER_CASE = sys.intern("latitude")
STR_DEPTH_LOWER_CASE = sys.intern("depth")

STATUS = "status"
MESSAGE = "message"
DATA = "data"
PARTITION_KEY = sys.intern("PARTITION_KEY")

PARQUET_SUBSET_ROW_NUMBER: int = 20000