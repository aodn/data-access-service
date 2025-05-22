from typing import List
import boto3
import zipfile
from io import BytesIO
import os


class ZipStreamingBody:
    def __init__(self, bucket_name: str, s3_keys: List[str]):
        self._bucket_name = bucket_name
        self._s3_keys = s3_keys
        self._index = 0
        self._zip_buffer = BytesIO()

    def _stream_zip(self):
        s3_client = boto3.client('s3')
        with zipfile.ZipFile(self._zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            while self._index < len(self._s3_keys):
                s3_key = self._s3_keys[self._index]
                self._index += 1

                # Download file from S3
                response = s3_client.get_object(Bucket=self._bucket_name, Key=s3_key)
                file_data = response['Body'].read()

                # Write file to ZIP
                zip_file.writestr(os.path.basename(s3_key), file_data)

        # Reset buffer for reading
        self._zip_buffer.seek(0)
        while chunk := self._zip_buffer.read(8192):  # Stream in chunks
            yield chunk

    def read(self, size=-1):
        if not hasattr(self, "_zip_stream"):
            self._zip_stream = self._stream_zip()

        try:
            return next(self._zip_stream)
        except StopIteration:
            return b''


if __name__ == "__main__":
    # AWS S3 configuration
    s3_client = boto3.client('s3')
    bucket_name = 'havier-example-bucket'
    s3_key = 'compressed_file.txt.zip'
    paths = ['t1.txt', 't1 - Copy.txt']

    # Stream and upload
    try:
        stream = ZipStreamingBody(bucket_name=bucket_name, s3_keys=paths)
        s3_client.upload_fileobj(stream, bucket_name, s3_key)
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
