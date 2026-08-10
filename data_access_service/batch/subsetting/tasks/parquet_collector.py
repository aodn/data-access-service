from typing import List

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)


def collect_parquet_files(master_job_id: str, subset_request: SubsetRequest):

    aws = AWSHelper()
    config = Config.get_config()
    log = init_log(config)
    bucket_name = config.get_subsetting_bucket_name()
    dataset: list[str] = aws.list_s3_folders(
        bucket_name=bucket_name, prefix=config.get_s3_temp_folder_name(master_job_id)
    )

    # We can have multiple dataset to the same UUID, they are export accordingly under different folder
    # so we need to scan each folder and depends on the folder name
    download_urls: List[str] = []
    for d in dataset:
        if d.endswith(".parquet"):
            p = aws.read_parquet_from_s3(
                f"s3://{bucket_name}/{config.get_s3_temp_folder_name(master_job_id)}{d}"
            )
            download_urls.append(
                aws.write_csv_to_s3(
                    p, bucket_name, f"{master_job_id}/{d.replace('.parquet', '')}.zip"
                )
            )
        else:
            log.warning("Skipping unrecognised output folder %s", d)

    subject = f"Finish processing data file whose uuid is:  {subset_request.uuid}"

    html_content = get_download_email_html_body(
        subset_request=subset_request, object_urls=download_urls
    )

    aws.send_email(
        recipient=subset_request.recipient, subject=subject, html_body=html_content
    )
    log.info("Finish aggregation and send email")
