from typing import List
import boto3
import zipfile
from io import BytesIO
import os

from data_access_service.core.AWSClient import AWSClient


class ZipStreamingBody:
    def __init__(self, bucket: str, s3_keys: List[str], s3: boto3.client):
        self._zip_stream = None
        self._bucket_name = bucket
        self._s3_keys = s3_keys
        self._zip_buffer = BytesIO()
        self._s3 = s3

    def _stream_zip(self, chunk_size):

        with zipfile.ZipFile(self._zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for key in self._s3_keys:

                # Download file from S3
                response = self._s3.get_object(Bucket=self._bucket_name, Key=key)
                file_data = response['Body'].read()
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
