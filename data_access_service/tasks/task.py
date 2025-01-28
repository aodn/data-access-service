import logging

from data_access_service import API, init_log
from data_access_service.core.AWSClient import AWSClient

log = logging.getLogger(__name__)


def process_csv_data_file(
    uuid, start_date, end_date, min_lat, max_lat, min_lon, max_lon
):
    init_log(logging.DEBUG)

    # for debug usage. just keep it for several days
    if uuid is None:
        uuid = "debug-uuid"
    if start_date is None:
        start_date = "debug-start-date"
    if end_date is None:
        end_date = "debug-end-date"
    if min_lat is None:
        min_lat = "-90"
    if max_lat is None:
        max_lat = "90"
    if min_lon is None:
        min_lon = "-180"
    if max_lon is None:
        max_lon = "180"

    if None in [uuid, start_date, end_date]:
        raise ValueError("One or more required arguments are None")

    aws = AWSClient()
    log.info("start " + uuid)

    recipient = "huaizhi.dai@utas.edu.au"
    subject = "start " + uuid
    content = "already start processing" + uuid + ". Please wait for the result. After the process is done, you will receive another email."

    aws.send_email(recipient, subject, content)

    csv_file_path = _generate_csv_file(
        end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid
    )

    s3_path = f"{uuid}/{csv_file_path}"

    aws.upload_data_file_to_s3(csv_file_path, s3_path)
    aws.send_email(recipient, "finish " + uuid, "The result is ready. You can download it")


def _generate_csv_file(end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid):

    data_frame = _query_data(
        end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid
    )

    csv_file_path = f"lat:{min_lat}~{max_lat}_lon:{min_lon}~{max_lon}_date:{start_date}~{end_date}.csv"
    data_frame.to_csv(csv_file_path, index=False)

    return csv_file_path


def _query_data(end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid):

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
    return data_frame
