#!/bin/sh

poetry install

poetry run python -c "
import os
from data_access_service.tasks.task import generate_csv_data_file

uuid = os.getenv('UUID')
start_time = os.getenv('START_TIME')
end_time = os.getenv('END_TIME')
min_lat = os.getenv('SOUTH')
max_lat = os.getenv('NORTH')
min_lon = os.getenv('WEST')
max_lon = os.getenv('EAST')

generate_csv_data_file(uuid, start_time, end_time, min_lat, max_lat, min_lon, max_lon)
"