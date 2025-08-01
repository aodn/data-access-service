import os

from data_access_service import Config
from data_access_service.core.AWSHelper import AWSHelper


def upload_all_files_in_folder_to_temp_s3(
    master_job_id: str, local_folder: str, aws: AWSHelper
) -> str:
    config: Config = Config.get_config()
    bucket_name = config.get_csv_bucket_name()
    s3_temp_folder = config.get_s3_temp_folder_name(master_job_id)

    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = f"{s3_temp_folder}{os.path.relpath(local_file_path, local_folder)}"
            aws.upload_file_to_s3(local_file_path, bucket_name, s3_key)

    return s3_temp_folder
