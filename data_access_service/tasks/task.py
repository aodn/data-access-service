# @restapi.route("/data/csv", methods=["POST"])
import boto3
import pandas as pd


def generate_csv_data_file(uuid, start_time, end_time, min_lat, max_lat, min_lon, max_lon):

    # for debug usage
    if uuid is None:
        uuid = "debug-uuid"
    if start_time is None:
        start_time = "debug-start-time"
    if end_time is None:
        end_time = "debug-end-time"
    if min_lat is None:
        min_lat = "debug-min-lat"
    if max_lat is None:
        max_lat = "debug-max-lat"
    if min_lon is None:
        min_lon = "debug-min-lon"
    if max_lon is None:
        max_lon = "debug-max-lon"


    if None in [uuid, start_time, end_time, min_lat, max_lat, min_lon, max_lon]:
        raise ValueError("One or more required arguments are None")

    print("start" + uuid)
    data = {
        "start_time": [start_time],
        "end_time": [end_time],
        "min_lat": [min_lat],
        "max_lat": [max_lat],
        "min_lon": [min_lon],
        "max_lon": [max_lon],
    }
    data_frame = pd.DataFrame(data)

    csv_file_path = "example1.csv"
    data_frame.to_csv(csv_file_path, index=False)

    s3 = boto3.client("s3")
    bucket_name = "havier-example-bucket"
    s3_path = f"example/{uuid}/example1.csv"
    try:
        s3.upload_file(csv_file_path, bucket_name, s3_path)
        print(f"File uploaded to s3://{bucket_name}/{s3_path}")
    except FileNotFoundError:
        print("The file was not found")

    return "aaa"

def another_function():
    return "bbb"