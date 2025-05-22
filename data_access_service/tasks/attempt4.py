from typing import List

import boto3
import zipfile
import os
from io import BytesIO

class ZipStreamingBody:
    def __init__(self, bucket_name: str, s3_keys: List[str]):  # 1MB chunks
        self._bucket_name = bucket_name
        self.file_paths = s3_keys
        self.zip_buffer = BytesIO()
        self._prepare_zip()

    def _prepare_zip(self):
        for file_path in self.file_paths:
            with zipfile.ZipFile(self.zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.write(file_path, os.path.basename(file_path))

        self.zip_buffer.seek(0)  # Reset buffer position for reading

    def read(self, size=-1):
        return self.zip_buffer.read(size)

    def _get_files_to_compress(self):



if __name__ == "__main__":
    # AWS S3 configuration
    s3_client = boto3.client('s3')
    bucket_name = 'havier-example-bucket'
    s3_key = 'compressed_file.txt.zip'
    path1 = 'file_0.txt'  # Your 2GB text file
    path2 = 'file_1.txt'  # Another file to be zipped
    path3 = 'file_2.txt'  # Another file to be zipped
    path4 = 'file_3.txt'  # Another file to be zipped
    path5 = 'file_4.txt'  # Another file to be zipped

    # Stream and upload
    try:
        stream = ZipStreamingBody([path1, path2, path3, path4, path5])
        s3_client.upload_fileobj(stream, bucket_name, s3_key)  # Pass the instance, not the generator
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
