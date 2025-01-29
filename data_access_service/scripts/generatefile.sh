#!/bin/sh

poetry install

poetry run python -c "
import os
from data_access_service.tasks.generate_csv_file import process_csv_data_file

uuid = os.getenv('UUID')
start_time = os.getenv('START_DATE')
end_time = os.getenv('END_DATE')
min_lat = os.getenv('MIN_LAT')
max_lat = os.getenv('MAX_LAT')
min_lon = os.getenv('MIN_LON')
max_lon = os.getenv('MAX_LON')
recipient = os.getenv('RECIPIENT')

process_csv_data_file(uuid, start_time, end_time, min_lat, max_lat, min_lon, max_lon, recipient)
"
