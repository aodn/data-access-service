from typing import List
import boto3
import zipfile
from io import BytesIO
import os
from botocore.config import Config

from data_access_service.core.AWSClient import AWSClient


class ZipStreamingBody:
    def __init__(self, bucket: str, s3_keys: List[str]):
        self._zip_stream = None
        self._bucket_name = bucket
        self._s3_keys = s3_keys
        self._index = 0
        self._zip_buffer = BytesIO()
        self._has_streamed_all = False

    #     temp size 1 GB limit:
    #     self._max_size = 1024 * 1024 * 1024  # 1 GB

    def _stream_zip(self, chunk_size):
        print("stream_zip")
        s3 = boto3.client('s3')

        with zipfile.ZipFile(self._zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            # while self._index < len(self._s3_keys):
            while self._index < len(self._s3_keys):
                key = self._s3_keys[self._index]
                self._index += 1

                # Download file from S3
                print(f"Downloading {key} from S3 bucket {self._bucket_name}...")
                response = s3.get_object(Bucket=self._bucket_name, Key=key)
                file_data = response['Body'].read()
                # Write file to ZIP
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
            print("read size is" + str(size))
            a =  next(self._zip_stream)
            print("returning in read() size is" + str(len(a)))
            return a
        except StopIteration:
            print("StopIteration")
            return b''


if __name__ == "__main__":
    # AWS S3 configuration
    s3_client = boto3.client('s3')
    bucket_name = 'havier-example-bucket'
    s3_key = 'compressed_file.txt.zip'

    aws = AWSClient()
    keys = aws.get_s3_keys(bucket_name=bucket_name, folder_prefix="data")
    print(keys)
    # Stream and upload
    try:
        stream = ZipStreamingBody(bucket=bucket_name, s3_keys= keys)
        s3_client.upload_fileobj(stream, bucket_name, s3_key)
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
