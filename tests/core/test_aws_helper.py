import shutil

import pandas as pd
import requests

import xarray as xr
from pathlib import Path
from unittest.mock import patch, MagicMock, ANY

from botocore.exceptions import ClientError
from data_access_service.config.config import IntTestConfig, Config
from data_access_service.core.AWSHelper import AWSHelper
from tests.core.test_with_s3 import TestWithS3


class TestAWSHelper(TestWithS3):

    def test_email_client_format(
        self, setup, aws_clients, localstack, mock_boto3_client
    ):
        _, _, ses_client = aws_clients
        config = Config.get_config()
        config.set_ses_client(ses_client)

        helper = AWSHelper()
        receipt = "receipt@test.com"
        subject = "This is a test"
        links = ["http://test/test.zip", "https://test/test1.zip"]

        # Need to setup email verify otherwise SES will reject sent
        ses_client.verify_email_identity(EmailAddress=config.get_sender_email())
        helper.send_email(receipt, subject, links)

        # Retrieve sent emails from LocalStack SES endpoint
        try:
            response = (
                ses_client.get_paginator("list_identities")
                .paginate()
                .build_full_result()
            )
            assert response["Identities"] == [
                config.get_sender_email()
            ], "Verified identities"
        except ClientError as e:
            print(f"Error listing identities: {e.response['Error']['Message']}")

        # Fetch email content from LocalStack SES endpoint
        try:
            response = requests.get(localstack.get_url() + "/_aws/ses")
            response.raise_for_status()  # Raise exception for HTTP errors
            emails = response.json().get("messages", [])

            if not emails:
                assert False, "No emails found in LocalStack SES."

            # Find the latest email (or filter by Message ID if needed)
            for email in emails:
                if (
                    email["Source"] == config.get_sender_email()
                    and email["Subject"] == subject
                ):
                    assert (
                        email["Source"] == config.get_sender_email()
                    ), "Source correct"
                    assert email["Destination"]["ToAddresses"] == [
                        receipt
                    ], "ToAddress correct"
                    assert email["Subject"] == subject, "Subject correct"
                    assert (
                        email["Body"]["text_part"]
                        == "Hello, User!\nPlease use the link below to download the files"
                    ), "Text correct"
                    assert (
                        email["Body"]["html_part"].strip()
                        == """<html>\n        <body>\n            [\'<a href="http://test/test.zip">http://test/test.zip</a>\', \'<a href="https://test/test1.zip">https://test/test1.zip</a>\']\n        </body>\n        </html>"""
                    ), "Html correct"
                else:
                    assert False, "Email not found in LocalStack SES."

        except requests.exceptions.RequestException as e:
            assert False, f"Error fetching emails: {e}"

    def test_safe_zarr_to_netcdf(
        self, setup, aws_clients, localstack, mock_boto3_client
    ):
        """
        Some Zarr datasets contain invalid Unicode surrogates in their metadata (stored in `.zattrs`), which cause `UnicodeEncodeError` when writing to NetCDF.
        `safe_zarr_to_netcdf` ignores these characters with utf-8 encode these attributes before writing to make sure the conversion works.
        """
        helper = AWSHelper()
        # get dataset with code
        # ds = aodn_dataset.get_data(date_start="2006-06-12", date_end="2006-06-12", lat_min=-70.0, lat_max=-69.9, lon_min=20.0, lon_max=20.1)
        zarr_dataset = "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"
        zarr_path = Path(__file__).parent.parent / "canned/s3_sample1" / zarr_dataset

        ds = xr.open_zarr(zarr_path, consolidated=False)

        # the original zarr file has invalid unicode characters in global attributes
        assert any(
            isinstance(v, str) and has_invalid_unicode(v) for v in ds.attrs.values()
        )

        helper.s3 = MagicMock()
        helper.s3.meta.region_name = "us-east-1"
        # the invalid characters should be processed within write_zarr_from_s3 function
        mock_url = "https://test-bucket.s3.us-east-1.amazonaws.com/test.nc"

        with patch.object(
            xr.Dataset,
            "to_netcdf",
            side_effect=UnicodeEncodeError(
                "utf-8", "bad surrogate", 0, 1, "surrogate not allowed"
            ),
        ):
            with patch.object(AWSHelper, "safe_zarr_to_netcdf") as mock_safe, patch(
                "data_access_service.core.AWSHelper.AWSHelper.upload_file_to_s3",
                return_value=mock_url,
            ) as mock_upload:
                url = helper.write_zarr_from_s3(ds, "test-bucket", "test.nc")

                # if UnicodeEncodeError occurred, the safe_zarr_to_netcdf should be called
                mock_safe.assert_called_once_with(ds, ANY)

    def test_write_accumulated_partitions_to_csv(
        self, setup, aws_clients, localstack, mock_boto3_client
    ):
        partitions = [
            pd.DataFrame(
                {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [1.1, 2.2, 3.3]}
            ),
            pd.DataFrame({"col1": [4, 5, 6], "col2": ["d", "e", "f"]}),
            pd.DataFrame({"col1": [7, 8, 9], "col2": ["x", "y", "z"]}),
        ]
        file_index = 0

        mock_zipfile = MagicMock()
        helper = AWSHelper()
        helper.write_accumulated_partitions_to_csv(partitions, mock_zipfile, file_index)

        mock_zipfile.writestr.assert_called_once()
        filename, csv_content = mock_zipfile.writestr.call_args[0]

        assert filename == "part_000000000.csv"
        # the expected result should have the concat partitions
        assert "z" in csv_content


def has_invalid_unicode(s: str) -> bool:
    return any(0xD800 <= ord(ch) <= 0xDFFF for ch in s)
