from data_access_service import Config
from data_access_service.core.AWSClient import AWSClient


def collect_data_files_from_shared_storage(s3_temp_folder: str, aws: AWSClient):

    #get all files from the s3_temp_folder
    config: Config = Config.get_config()
    bucket_name = config.get_csv_bucket_name()

