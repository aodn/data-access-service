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
        self._populate_buffer(buffer=self._zip_buffer, chunk_size=chunk_size, s3=s3)

        # Reset buffer for reading
        self._zip_buffer.seek(0)
        print("buffer1 size is:")
        print(self._zip_buffer.getbuffer().nbytes / (1024 * 1024))


        while self._zip_buffer.getbuffer().nbytes > 0 :
            yield self.generate_chunk(chunk_size)


    def generate_chunk(self, chunk_size):

        if self._has_streamed_all:
            print("both buffers are empty")
            return b''

        chunk = self.get_chunk_from_(buffer=self._zip_buffer, chunk_size=chunk_size)
        if len(chunk) == chunk_size:
            print("Chunk size is equal to chunk_size")
            return chunk
        elif len(chunk) > chunk_size:
            raise Exception("Chunk size is greater than chunk_size")
        else:
            print("Chunk size is less than chunk_size")
            if self._index < len(self._s3_keys):
                self._zip_buffer = BytesIO()
                self._populate_buffer(buffer=self._zip_buffer, chunk_size=chunk_size, s3=boto3.client('s3'))

                size_to_read = chunk_size - len(chunk)
                print(f"pointer position is {self._zip_buffer.tell()}")
                chunk_2 = self._zip_buffer.read(chunk_size - len(chunk))
                return chunk + chunk_2
            else:
                self._has_streamed_all = True
                print("No more files to process. return the final chunk")
                return chunk


    def get_chunk_from_(self, buffer, chunk_size):
        return buffer.read(chunk_size);
        # while chunk := buffer.read(chunk_size):  # Stream in chunks
        #     print("Yielding chunk of size:", len(chunk))
        #     print("buffer size is:")
        #     print(self._zip_buffer_1.getbuffer().nbytes / (1024 * 1024))
        #     yield chunk
            # remaining_data = self._zip_buffer_1.read()
            # self._zip_buffer_1.seek(0)
            # self._zip_buffer_1.truncate(0)
            # self._zip_buffer_1.write(remaining_data)
            # self._zip_buffer_1.seek(0)

    def _populate_buffer(self, buffer: BytesIO,  chunk_size: int, s3: boto3.client):
        with zipfile.ZipFile(buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            # while self._index < len(self._s3_keys):
            while buffer.getbuffer().nbytes < chunk_size:
                if self._index >= len(self._s3_keys):
                    print("No more files to process.")
                    break
                key = self._s3_keys[self._index]
                self._index += 1

                # Download file from S3
                print(f"Downloading {key} from S3 bucket {self._bucket_name}...")
                response = s3.get_object(Bucket=self._bucket_name, Key=key)
                file_data = response['Body'].read()

                print("before writing, buffer size is: ")
                print(buffer.getbuffer().nbytes / (1024 * 1024))
                # Write file to ZIP
                zip_file.writestr(os.path.basename(key), file_data)
                print("after writing, buffer size is:")
                print(buffer.getbuffer().nbytes / (1024 * 1024))
        buffer.seek(0)

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
    s3_client = boto3.client(
        's3',
        config=Config(
            connect_timeout=3600,  # Increase connection timeout (default is 10 seconds)
            read_timeout=3600  # Increase read timeout (default is 60 seconds)
        )
    )
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
