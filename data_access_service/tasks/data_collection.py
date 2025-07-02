import os
import zipfile
import dask.dataframe as dd

from io import BytesIO
from typing import List

from data_access_service import Config
from data_access_service.core.AWSHelper import AWSHelper


def collect_data_files(master_job_id: str, dataset_uuid: str, recipient: str):

    aws = AWSHelper()
    config = Config.get_config()
    bucket_name = config.get_csv_bucket_name()
    dataset: list[str] = aws.list_s3_folders(
        bucket_name=bucket_name, prefix=config.get_s3_temp_folder_name(master_job_id)
    )

    # We can have multiple dataset to the same UUID, they are export accordingly under different folder
    # so we need to scan each folder and depends on the folder name
    download_url = None
    for d in dataset:
        if d.endswith(".parquet"):
            p = aws.read_parquet_from_s3(
                f"s3://{bucket_name}/{config.get_s3_temp_folder_name(master_job_id)}{d}"
            )
            download_url = aws.write_csv_to_s3(
                p, bucket_name, f"{master_job_id}/{d.replace('.parquet', '')}.zip"
            )

    subject = f"Finish processing data file whose uuid is:  {dataset_uuid}"
    body_text = (
        f"You can download the data file from the following link: {download_url}"
    )
    aws.send_email(recipient=recipient, subject=subject, body_text=body_text)


class ZipStreamingBody:
    def __init__(self, bucket: str, s3_keys: List[str], aws: AWSHelper):
        self._zip_stream = None
        self._bucket_name = bucket
        self._s3_keys = s3_keys
        self._zip_buffer = BytesIO()
        self._aws = aws

    def _stream_zip(self, chunk_size):

        with zipfile.ZipFile(
            self._zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zip_file:
            for key in self._s3_keys:

                # Download file from S3
                file_data = self._aws.get_s3_object(
                    bucket_name=self._bucket_name, s3_key=key
                )
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
            return b""
