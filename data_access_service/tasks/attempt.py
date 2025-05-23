from typing import List
import boto3
import zipfile
from io import BytesIO
import os
from botocore.config import Config

from data_access_service.core.AWSClient import AWSClient


class ZipStreamingBody:
    def __init__(self, bucket_name: str, s3_keys: List[str]):
        self._zip_stream = None
        self._bucket_name = bucket_name
        self._s3_keys = s3_keys
        self._index = 0
        self._zip_buffer = BytesIO()

    def _stream_zip(self, chunk_size=1024 * 1024):
        print("stream_zip")
        s3_client = boto3.client('s3')
        with zipfile.ZipFile(self._zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            while self._index < len(self._s3_keys):
                s3_key = self._s3_keys[self._index]
                self._index += 1

                # Download file from S3
                print(f"Downloading {s3_key} from S3 bucket {self._bucket_name}...")
                response = s3_client.get_object(Bucket=self._bucket_name, Key=s3_key)
                file_data = response['Body'].read()

                # Write file to ZIP
                zip_file.writestr(os.path.basename(s3_key), file_data)
                print("buffer size is:")
                print(self._zip_buffer.getbuffer().nbytes / (1024 * 1024))

        # Reset buffer for reading
        self._zip_buffer.seek(0)
        print("buffer size is:")
        print(self._zip_buffer.getbuffer().nbytes / (1024 * 1024))
        while chunk := self._zip_buffer.read(chunk_size):  # Stream in chunks
            print("Yielding chunk of size:", len(chunk))
            print("buffer size is:")
            print(self._zip_buffer.getbuffer().nbytes / (1024 * 1024))
            yield chunk
            remaining_data = self._zip_buffer.read()
            self._zip_buffer.seek(0)
            self._zip_buffer.truncate(0)
            self._zip_buffer.write(remaining_data)
            self._zip_buffer.seek(0)

    def read(self, size):
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
    s3_client = boto3.client(
        's3',
        config=Config(
            connect_timeout=3600,  # Increase connection timeout (default is 10 seconds)
            read_timeout=3600  # Increase read timeout (default is 60 seconds)
        )
    )
    bucket_name = 'havier-example-bucket'
    s3_key = 'compressed_file.txt.zip'
    paths = [
        't1.txt',
        't1 - Copy.txt',
        'example_file.txt',
        # 'example_file.csv',
        # 'Book1.csv',
        # 'compressed_file.txt.zip'
    ]

    aws = AWSClient()
    keys = aws.get_s3_keys(bucket_name=bucket_name, folder_prefix="data")
    print(keys)
    # Stream and upload
    try:
        stream = ZipStreamingBody(bucket_name=bucket_name, s3_keys=paths + keys[:1])
        s3_client.upload_fileobj(stream, bucket_name, s3_key)
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
