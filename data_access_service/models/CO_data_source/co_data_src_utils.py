from aodn_cloud_optimised.lib.DataQuery import GetAodn
import logging

import requests

log = logging.getLogger(__name__)


def get_csiro_co_data_src() -> GetAodn:
    """
    Currently, this "Marine National Facility (MNF) Processed Underway Parquet Data" is the only external
    cloud optimized dataset we are integrating with, so there are a lot of hard coded values and assumptions in the code.
    It needs refactoring when there are more external cloud optimized datasets.
    """

    # this one is only for dataset "Marine National Facility (MNF) Processed Underway Parquet Data"
    CSIRO_KEY_REQUEST_URL = (
        "https://data.csiro.au/dap/api/v2/collections/72626/files/s3"
    )
    log.info("Requesting temporary access keys for CSIRO cloud optimized dataset...")
    response = requests.get(CSIRO_KEY_REQUEST_URL, timeout=10)
    log.info(
        "Received response from CSIRO for temporary access keys, status code: %s",
        response.status_code,
    )
    if response.status_code == 200:
        res = response.json()
        access_key = res["accessKey"]
        secret_access_key = res["secretAccessKey"]
        endpoint_url = res["endPointUrl"]
        bucket_name = res["bucket"]
        remote_directory = res["remoteDirectory"]
        if remote_directory.startswith(bucket_name + "/"):
            prefix = remote_directory[len(bucket_name) + 1 :] + "data/"
        else:
            raise Exception(f"Unexpected remote directory format: {remote_directory}")

        params = {
            "bucket_name": bucket_name,
            "prefix": prefix,
            "key": access_key,
            "secret": secret_access_key,
            "endpoint_url": endpoint_url,
        }
        csiro = GetAodn(
            bucket_name=params["bucket_name"],
            prefix=params["prefix"],
            s3_fs_opts={
                "key": params["key"],
                "secret": params["secret"],
                "client_kwargs": {"endpoint_url": params["endpoint_url"]},
            },
        )
        log.info(
            "Successfully initialized CSIRO cloud optimized data source with temporary access keys"
        )
        return csiro

    else:
        raise Exception(
            f"Failed to get keys from CSIRO, status code: {response.status_code}"
        )
