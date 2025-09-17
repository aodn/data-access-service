import csv
import io
import os
import shutil
import tempfile
import zipfile
import dask.dataframe
import dask.dataframe as dd
import xarray
import pandas as pd

from pathlib import Path
from typing import List
from data_access_service import init_log
from data_access_service.config.config import Config, IntTestConfig
from io import BytesIO
from data_access_service.core.constants import PARTITION_KEY, MAX_CSV_ROW


class AWSHelper:

    def __init__(self):
        self.config: Config = Config.get_config()
        self.log = init_log(self.config)
        self.log.info("Init AWS class")
        self.s3 = self.config.get_s3_client()
        self.ses = self.config.get_ses_client()
        self.batch = self.config.get_batch_client()

    def get_storage_options(self):
        # Special handle for testing
        if isinstance(self.config, IntTestConfig):
            # We need to point it to the test s3
            return {
                "client_kwargs": {
                    "endpoint_url": self.s3.meta.endpoint_url,
                    "region_name": self.s3.meta.region_name,
                },
                "key": IntTestConfig.get_s3_test_key(),
                "secret": IntTestConfig.get_s3_secret(),
            }
        else:
            return None

    def write_csv_to_s3(
        self, data: dask.dataframe.DataFrame, bucket_name: str, key: str
    ) -> str:
        target = data.drop(PARTITION_KEY, axis=1).reset_index(drop=True)
        # the max row should be the max excel row limit exclude the header row
        max_excel_row = MAX_CSV_ROW - 1
        # Get total row count
        total_rows = target.shape[0].compute()

        # Create temporary directory with tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            zip_path = temp_dir_path / "output.zip"
            try:
                with zipfile.ZipFile(
                        zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
                ) as zf:
                    accumulated_partitions = []
                    total_rows = 0
                    csv_file_index = 0

                    for i, partition in enumerate(target.to_delayed()):
                        try:
                            # Compute one partition
                            partition_df = partition.compute()
                            partition_rows = len(partition_df)

                            # Check if adding this partition would exceed Excel row limit
                            #  save the accumulated partitions to csv under max excel row
                            if total_rows + partition_rows > max_excel_row and accumulated_partitions:
                                # Convert accumulated partitions to CSV
                                self._write_accumulated_partitions_to_csv(
                                    accumulated_partitions, zf, csv_file_index
                                )
                                csv_file_index += 1
                                # Reset accumulators
                                accumulated_partitions = []
                                total_rows = 0

                            # Add current partition to accumulator
                            accumulated_partitions.append(partition_df)
                            total_rows += partition_rows

                            self.log.info(f"Accumulated partition {i}, total rows: {total_rows}")

                            # Clean up partition reference
                            del partition_df

                        except Exception as e:
                            self.log.error(f"Error processing partition {i}: {e}")
                            raise

                    # Handle remaining accumulated partitions
                    if accumulated_partitions:
                        self._write_accumulated_partitions_to_csv(
                            accumulated_partitions, zf, csv_file_index
                        )

                self.upload_file_to_s3(str(zip_path), bucket_name, key)

            except Exception as e:
                self.log.error(f"Error creating ZIP: {e}")
                raise

            region = self.s3.meta.region_name
            return f"https://{bucket_name}.s3.{region}.amazonaws.com/{key}"

    def write_zarr_from_s3(self, data: xarray.Dataset, bucket_name: str, key: str):
        # Save to temporary local file
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=True) as temp_file:
            try:
                data.to_netcdf(
                    temp_file.name,
                    engine="netcdf4",
                )
            except UnicodeEncodeError:
                # Work around an issue where some attribute is having not supported encode, we need to
                # set it back to utf-8 for str
                self.safe_zarr_to_netcdf(data, temp_file.name)
            helper = AWSHelper()
            helper.upload_file_to_s3(temp_file.name, bucket_name, key)

            region = self.s3.meta.region_name
            return f"https://{bucket_name}.s3.{region}.amazonaws.com/{key}"

    def read_parquet_from_s3(self, file_path: str):
        return dd.read_parquet(
            file_path,
            engine="pyarrow",
            blocksize="512M",
            storage_options=self.get_storage_options(),
        )

    def upload_file_to_s3(self, file_path: str, s3_bucket: str, s3_key: str) -> str:
        """
        Upload a file to an S3 bucket. Must be a file, not a file-like object.
        Args:
            file_path: Path to the file to upload.
            s3_bucket: Name of the S3 bucket.
            s3_key: Key under which to store the file in S3.
        """

        # Validate file
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Upload to S3
        self.s3.upload_file(file_path, s3_bucket, s3_key)
        region = self.s3.meta.region_name
        object_download_url = f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
        return object_download_url

    def upload_fileobj_to_s3(self, file_obj: any, s3_bucket: str, s3_key: str) -> str:
        """
        Upload a file-like object to an S3 bucket.
        Args:
            file_obj: A file-like object to upload.
            s3_bucket: Name of the S3 bucket.
            s3_key: Key under which to store the file in S3.
        """
        try:
            self.s3.upload_fileobj(file_obj, s3_bucket, s3_key)
            region = self.s3.meta.region_name
            object_download_url = (
                f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
            )
            return object_download_url
        except Exception as e:
            self.log.error(f"Error uploading file object to S3: {e}")
            raise e

    def send_email(self, recipient: str, subject: str, download_urls: List[str]):

        # Text and HTML parts
        text_part = "Hello, User!\nPlease use the link below to download the files"

        if not download_urls:
            html_content = "<p>No data found for your selected subset.</p>"
        else:
            html_content = ['<a href="' + l + '">' + l + "</a>" for l in download_urls]
        html_part = f"""
        <html>
        <body>
            {html_content}
        </body>
        </html>
        """

        try:
            response = self.ses.send_email(
                Source=self.config.get_sender_email(),
                Destination={"ToAddresses": [recipient]},
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": text_part, "Charset": "UTF-8"},
                        "Html": {"Data": html_part, "Charset": "UTF-8"},
                    },
                },
            )
            self.log.info(
                f"Email sent to {recipient} with message ID: {response['MessageId']}"
            )
            return response
        except Exception as e:
            self.log.info(f"Error sending email to {recipient}: {e}")
            raise e

    def submit_a_job(
        self,
        job_name: str,
        job_queue: str,
        job_definition: str,
        parameters: dict,
        array_size: int = 1,
        dependency_job_id: str = None,
    ) -> str:
        """
        Submit a job to AWS Batch.

        Args:
            job_name: Name of the job.
            job_queue: Job queue to submit the job to.
            job_definition: Job definition to use.
            parameters: Parameters for the job.
            array_size: Size of the array job (default is 0, which means no array job).
            dependency_job_id: Job ID of the job this job depends on (default is None).

        Returns:
            The response from the AWS Batch service.
        """
        request = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "parameters": parameters,
        }

        if array_size > 1:
            request["arrayProperties"] = {"size": array_size}

        if dependency_job_id:
            request["dependsOn"] = [{"jobId": dependency_job_id}]
        response = self.batch.submit_job(**request)

        self.log.info(f"Job submitted: {response['jobId']}")
        # return job id
        return response["jobId"]

    def get_s3_keys(self, bucket_name: str, folder_prefix: str) -> list:
        """
        Retrieve all S3 keys in a specified folder within a bucket. (no folders, only files)
        Args:
            bucket_name: Name of the S3 bucket.
            folder_prefix: Prefix of the folder to list keys from.
        """
        keys = []
        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": bucket_name,
                "Prefix": folder_prefix,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = self.s3.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                for obj in response["Contents"]:
                    # Filter out folders (if any)
                    if obj["Key"].endswith("/"):
                        continue
                    keys.append(obj["Key"])

            if response.get("IsTruncated"):  # Check if there are more keys to fetch
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return keys

    def get_s3_object(self, bucket_name: str, s3_key: str) -> bytes | None:
        """
        Retrieve an object from S3 bucket by its key.
        Args:
            bucket_name: Name of the S3 bucket.
            s3_key: Key of the object to retrieve.

        Returns:
            The content of the object as bytes, or None if the object does not exist.
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            return response["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            self.log.error(f"Object {s3_key} not found in bucket {bucket_name}.")
            return None

    # List top-level folders
    def list_s3_folders(self, bucket_name: str, prefix="", delimiter="/") -> list[str]:
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        folders = []
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter
        )
        for page in pages:
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    folder = (
                        common_prefix["Prefix"]
                        .removeprefix(prefix)
                        .replace(delimiter, "")
                    )
                    folders.append(folder)
        return folders

    def list_all_s3_objects(self, bucket_name: str, prefix: str = "") -> list[str]:
        """
        List all objects in an S3 bucket with a specific prefix.
        Args:
            bucket_name: Name of the S3 bucket.
            prefix: Prefix to filter objects by.

        Returns:
            A list of object keys in the specified bucket and prefix.
        """
        objects = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects.append(obj["Key"])
        return objects

    def extract_zip_from_s3(
        self, bucket_name: str, zip_key: str, output_path: str
    ) -> list[str]:
        # Retrieve the ZIP file from S3
        zip_obj = self.s3.get_object(Bucket=bucket_name, Key=zip_key)
        zip_data = BytesIO(zip_obj["Body"].read())

        # Calculate the total uncompressed size
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            extracted_files = [
                name for name in zip_ref.namelist() if not name.endswith("/")
            ]
            zip_ref.extractall(output_path)
            return extracted_files

    def get_object_size_from_s3(self, bucket_name, object_key):
        try:
            response = self.s3.head_object(Bucket=bucket_name, Key=object_key)
            return response["ContentLength"]
        except self.s3.exceptions.ClientError as e:
            raise ValueError(
                f"Error retrieving object size for {object_key}: {e}"
            ) from e

    @staticmethod
    def read_multipart_zarr_from_s3(file_path: str) -> xarray.Dataset:
        """
        The s3 connection need to set via mock because the mfdataset call the
        get_fs_token_paths where argument cannot be pass via s3_client. Hence, it is
        expect you call the mock_get_fs_token_paths because call this function
        :param file_path:
        :return:
        """
        return xarray.open_mfdataset(
            file_path,
            engine="zarr",
            combine="nested",
            consolidated=False,  # Must be false as the file is not consolidated_metadata()
            parallel=False,
        )

    @staticmethod
    def safe_zarr_to_netcdf(
        ds: xarray.Dataset, file_path: str, engine="netcdf4"
    ) -> None:
        """
        Write Zarr to NetCDF safely by cleaning invalid Unicode surrogates from attributes before writing.
        Args:
            ds: xarray Dataset to write.
            file_path: Path of the output NetCDF file.
            engine: NetCDF engine (default "netcdf4").
        """
        for k, v in ds.attrs.items():
            if isinstance(v, str):
                ds.attrs[k] = v.encode("utf-8", errors="ignore").decode("utf-8")
        ds.to_netcdf(file_path, engine=engine)

    @staticmethod
    def get_free_space(path: str) -> int:
        """
        Get the free space (in int bytes) of a temp directory.
        Args:
            path: Path to the temporary directory.
        Returns:
            int free space.
        """
        usage = shutil.disk_usage(path)
        return usage.free

    def _write_accumulated_partitions_to_csv(
            self, partitions: list, zf: zipfile.ZipFile, file_index: int
    ) -> None:
        """Helper method to write accumulated partitions to a single CSV file in the ZIP"""
        try:
            # Concatenate all accumulated partitions
            combined_df = pd.concat(partitions, ignore_index=True)

            # Convert to CSV string in memory
            csv_buffer = io.StringIO()
            combined_df.to_csv(
                csv_buffer,
                escapechar="\\",  # Escape special characters
                quoting=csv.QUOTE_NONNUMERIC,  # Quote non-numeric fields
                index=False,
            )
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Write to ZIP stream as CSV file
            filename = f"part_{file_index:09d}.csv"
            zf.writestr(filename, csv_content)
            self.log.info(f"Added {filename} to ZIP with {len(combined_df)} rows")

            # Clean up
            del combined_df
            del csv_content

        except Exception as e:
            self.log.error(f"Error writing accumulated partitions to CSV: {e}")
            raise