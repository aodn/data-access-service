import gzip
import pandas as pd
import logging

from datetime import timedelta, datetime
from io import BytesIO
from typing import Optional
from aodn_cloud_optimised import ParquetDataQuery
from data_access_service.core.descriptor import Depth, Descriptor

log = logging.getLogger(__name__)


def _extract_depth(data: dict):
    # We need to extract depth info
    depth = data.get("DEPTH")

    if depth is not None:
        return Depth(depth.get("valid_min"), depth.get("valid_max"), depth.get("units"))
    else:
        return None


def gzip_compress(data):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data.encode("utf-8"))
    return buf.getvalue()


class API:
    def __init__(self):
        log.info("Init parquet data query instance")

        self._raw = dict()
        self._cached = dict()

        # UUID to metadata mapper and init it, a scheduler need to
        # updated it as times go
        self._instance = ParquetDataQuery.GetAodn()
        self._metadata = self._instance.get_metadata()
        self._create_uuid_dataset_map()

        log.info("Done init")

    # Do not use cache, so that we can refresh it again
    def _create_uuid_dataset_map(self):
        # A map contains dataset name and Metadata class, which is not
        # so useful in our case, we need UUID
        catalog = self._metadata.metadata_catalog_uncached()

        for key in catalog:
            data = catalog.get(key)
            uuid = data.get("dataset_metadata").get("metadata_uuid")

            if uuid is not None and uuid != "":
                log.info("Adding uuid " + uuid + " name " + key)
                self._raw[uuid] = data
                self._cached[uuid] = Descriptor(
                    uuid=uuid, dname=key, depth=_extract_depth(data)
                )
            else:
                log.error("Data not found for dataset " + key)

    def get_mapped_meta_data(self, uuid: str):
        value = self._cached.get(uuid)

        if value is not None:
            return value
        else:
            return Descriptor(uuid=uuid)

    def get_raw_meta_data(self, uuid: str):
        value = self._raw.get(uuid)

        if value is not None:
            return value
        else:
            return None

    def has_data(self, uuid: str, start_date: datetime, end_date: datetime):
        md: Descriptor = self._cached.get(uuid)
        if md is not None:
            ds: ParquetDataQuery.Dataset = self._instance.get_dataset(md.dname)
            while start_date <= end_date:

                # currently use 366 days as a period, to make sure 1 query can cover 1 year
                period_end = start_date + timedelta(days=366)
                log.info(f"Checking data for {start_date} to {period_end}")
                if period_end > end_date:
                    period_end = end_date

                if not ds.get_data(
                    start_date, period_end, None, None, None, None, None
                ).empty:
                    return True
                else:
                    start_date = period_end + timedelta(days=1)
        return False

    def get_dataset_data(
        self,
        uuid: str,
        date_start=None,
        date_end=None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
    ) -> Optional[pd.DataFrame]:
        md: Descriptor = self._cached.get(uuid)

        if md is not None:
            ds: ParquetDataQuery.Dataset = self._instance.get_dataset(md.dname)

            # Default get 10 days of data
            if date_start is None:
                date_start = datetime.now() - timedelta(days=10)

            return ds.get_data(
                date_start, date_end, lat_min, lat_max, lon_min, lon_max, scalar_filter
            )
        else:
            return None
