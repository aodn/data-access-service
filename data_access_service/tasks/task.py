import boto3

from data_access_service import API
from data_access_service.config.config import load_config


def generate_csv_data_file(uuid, start_date, end_date, min_lat, max_lat, min_lon, max_lon):

    config = load_config()

    # for debug usage. just keep it for several days
    if uuid is None:
        uuid = "debug-uuid"
    if start_date is None:
        start_date = "debug-start-date"
    if end_date is None:
        end_date = "debug-end-date"
    if min_lat is None:
        min_lat = "debug-min-lat"
    if max_lat is None:
        max_lat = "debug-max-lat"
    if min_lon is None:
        min_lon = "debug-min-lon"
    if max_lon is None:
        max_lon = "debug-max-lon"


    if None in [uuid, start_date, end_date]:
        raise ValueError("One or more required arguments are None")

    print("start " + uuid)

    api = API()

    data_frame = api.get_dataset_data(
        uuid=uuid,
        date_start=start_date,
        date_end=end_date,
        lat_min=min_lat,
        lat_max=max_lat,
        lon_min=min_lon,
        lon_max=max_lon,
    )

    if data_frame is None or data_frame.empty:
        raise ValueError("No data found for the given parameters")

    csv_file_path = f"lat:{min_lat}~{max_lat}_lon:{min_lon}~{max_lon}_date:{start_date}~{end_date}.csv"
    data_frame.to_csv(csv_file_path, index=False)

    s3 = boto3.client("s3")
    bucket_name = config["aws"]["s3"]["bucket_name"]["csv"]
    s3_path = f"{uuid}/{csv_file_path}"
    try:
        s3.upload_file(csv_file_path, bucket_name, s3_path)
        print(f"File uploaded to s3://{bucket_name}/{s3_path}")
    except FileNotFoundError:
        print("The file was not found")

    return "aaa"

def another_function():
    return "bbb"