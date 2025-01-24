import boto3

from data_access_service.config.config import load_config


class AWSClient:
    def __init__(self):
        print("Init AWS class")
        self.s3 = boto3.client("s3")
        self.config = load_config()

    def upload_data_file_to_s3(self, file_path, s3_path):
        bucket_name = self.config["aws"]["s3"]["bucket_name"]["csv"]

        try:
            self.s3.upload_file(file_path, bucket_name, s3_path)
            print(f"File uploaded to s3://{bucket_name}/{s3_path}")
        except Exception as e:
            print(f"Error uploading file to s3://{bucket_name}/{s3_path}: {e}")
            raise e
