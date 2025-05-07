import os
from pathlib import Path


# Util function to upload canned test data to localstack s3 or any s3
def upload_to_s3(s3_client, bucket_name, sub_folder):
    for root, _, files in os.walk(sub_folder):
        for file in files:
            local_path = Path(root) / file
            # Compute S3 key relative to TEST_DATA_FOLDER
            relative_path = local_path.relative_to(sub_folder)
            s3_key = f"{relative_path}"
            s3_client.upload_file(str(local_path), bucket_name, s3_key)
            print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")


def delete_object_in_s3(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
        print(f"Deleted {len(objects)} objects from s3://{bucket_name}")
