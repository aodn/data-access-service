import gzip

import pandas as pd
import logging

from datetime import timedelta, datetime, timezone
from io import BytesIO
from typing import Optional
from aodn_cloud_optimised import DataQuery
from aodn_cloud_optimised.lib.config import get_notebook_url
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


class BaseAPI:
    def get_temporal_extent(self, uuid: str) -> (datetime, datetime):
        pass


class API(BaseAPI):
    def __init__(self):
        # the ready flag used to check API status
        self._is_ready = False
        log.info("Init parquet data query instance")

        self._raw = dict()
        self._cached = dict()

        # UUID to metadata mapper and init it, a scheduler need to
        # updated it as times go
        self._instance = DataQuery.GetAodn()
        self._metadata = self._instance.get_metadata()
        self._create_uuid_dataset_map()

        log.info("Done init")
        # init finalised, set as ready
        self._is_ready = True

    def get_api_status(self) -> bool:
        # used for checking if the API instance is ready
        return self._is_ready

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

    def get_mapped_meta_data(self, uuid: str | None):
        if uuid is not None:
            value = self._cached.get(uuid)
        else:
            value = self._cached.values()

        if value is not None:
            return value
        else:
            return Descriptor(uuid=uuid)

    def get_raw_meta_data(self, uuid: str) -> dict:
        value = self._raw.get(uuid)

        if value is not None:
            return value
        else:
            return dict()

    """
    Given a time range, we find if this uuid temporal cover the whole range
    """

    def has_data(self, uuid: str, start_date: datetime, end_date: datetime):
        md: Descriptor = self._cached.get(uuid)
        if md is not None:
            ds: DataQuery.Dataset = self._instance.get_dataset(md.dname)
            te = ds.get_temporal_extent()
            return start_date <= te[0] and te[1] <= end_date
        return False

    def get_temporal_extent(self, uuid: str) -> (datetime, datetime):
        md: Descriptor = self._cached.get(uuid)
        if md is not None:
            ds: DataQuery.Dataset = self._instance.get_dataset(md.dname)
            return ds.get_temporal_extent()
        else:
            return ()

    def map_column_names(
        self, uuid: str, columns: list[str] | None
    ) -> list[str] | None:

        if columns is None:
            return columns

        meta = self.get_raw_meta_data(uuid)
        output = list()
        for column in columns:
            # You want TIME field but not in there, try map to something else
            if column.casefold() == "TIME".casefold() and (
                "TIME" not in meta or "time" not in meta
            ):
                if "JULD" in meta:
                    output.append("JULD")
                elif "timestamp" in meta:
                    output.append("timestamp")
            else:
                output.append(column)

        return output

    def get_dataset_data(
        self,
        uuid: str,
        date_start: datetime = None,
        date_end: datetime = None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
        columns: list[str] = None,
    ) -> Optional[pd.DataFrame]:
        md: Descriptor = self._cached.get(uuid)

        if md is not None:
            ds: DataQuery.Dataset = self._instance.get_dataset(md.dname)

            # Default get 10 days of data
            if date_start is None:
                date_start = datetime.now(timezone.utc) - timedelta(days=10)
            else:
                if date_start.tzinfo is None:
                    date_start = pd.to_datetime(date_start).tz_localize(timezone.utc)
                else:
                    date_start = pd.to_datetime(date_start).tz_convert(timezone.utc)

            if date_end is None:
                date_end = datetime.now(timezone.utc)
            else:
                if date_end.tzinfo is None:
                    date_end = pd.to_datetime(date_end).tz_localize(timezone.utc)
                else:
                    date_end = pd.to_datetime(date_end).tz_convert(timezone.utc)

            # The get_data call the pyarrow and compare only works with non timezone datetime
            # now make sure the timezone is correctly convert to utc then remove it.
            # As get_date datetime are all utc, but the pyarrow do not support compare of datetime vs
            # datetime with timezone.
            if date_start.tzinfo is not None:
                date_start = date_start.astimezone(timezone.utc).replace(tzinfo=None)

            if date_end.tzinfo is not None:
                date_end = date_end.astimezone(timezone.utc).replace(tzinfo=None)

            try:
                return ds.get_data(
                    date_start,
                    date_end,
                    lat_min,
                    lat_max,
                    lon_min,
                    lon_max,
                    scalar_filter,
                    self.map_column_names(uuid, columns),
                )
            except ValueError as e:
                log.error(f"Error: {e}")
                return None
        else:
            return None

    @staticmethod
    def get_notebook_from(uuid: str) -> str:
        return get_notebook_url(uuid)
