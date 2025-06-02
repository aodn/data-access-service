import os
import zipfile
from io import BytesIO
from typing import List

from data_access_service import Config
from data_access_service.core.AWSClient import AWSClient


def collect_data_files(master_job_id: str, dataset_uuid: str, recipient: str):

    aws = AWSClient()
    bucket_name = Config.get_config().get_csv_bucket_name()
    s3_key_of_data_files = aws.get_s3_keys(bucket_name=bucket_name, folder_prefix=f"{master_job_id}/temp")

    stream = ZipStreamingBody(bucket=bucket_name, s3_keys=s3_key_of_data_files, aws=aws)
    download_url = aws.upload_fileobj_to_s3(file_obj=stream, s3_bucket=bucket_name, s3_key=f"{master_job_id}/data.zip")

    subject = f"Finish processing data file whose uuid is:  {dataset_uuid}"
    body_text = f"You can download the data file from the following link: {download_url}"
    aws.send_email(recipient=recipient, subject=subject, body_text=body_text)

class ZipStreamingBody:
    def __init__(self, bucket: str, s3_keys: List[str], aws: AWSClient):
        self._zip_stream = None
        self._bucket_name = bucket
        self._s3_keys = s3_keys
        self._zip_buffer = BytesIO()
        self._aws = aws

    def _stream_zip(self, chunk_size):

        with zipfile.ZipFile(self._zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for key in self._s3_keys:

                # Download file from S3
                file_data = self._aws.get_s3_object(bucket_name=self._bucket_name, s3_key=key)
                # Write the file to ZIP
                zip_file.writestr(os.path.basename(key), file_data)

        # Reset buffer for reading
        self._zip_buffer.seek(0)
        while True:
            data = self._zip_buffer.read(chunk_size)
            if not data:
                break
            yield data


    def read(self, size=-1):
        if self._zip_stream is None:
            self._zip_stream = self._stream_zip(chunk_size=size)

        try:
            return next(self._zip_stream)
        except StopIteration:
            return b''

