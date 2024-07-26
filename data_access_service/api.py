from aodn_cloud_optimised import ParquetDataQuery
from .descriptor import Depth, Descriptor
import logging

log = logging.getLogger(__name__)


def _extract_depth(data: dict):
    # We need to extract depth info
    depth = data.get('DEPTH')

    if depth is not None:
        return Depth(depth.get('valid_min'), depth.get('valid_max'), depth.get('units'))
    else:
        return None


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
            uuid = data.get('dataset_metadata').get('metadata_uuid')

            if uuid is not None and uuid != '':
                log.info("Adding uuid " + uuid + " name " + key)
                self._raw[uuid] = data
                self._cached[uuid] = Descriptor(uuid=uuid, key=key, depth=_extract_depth(data))
            else:
                log.error('Data not found for dataset ' + key)

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

    def get_dataset_data(self,
                         uuid: str,
                         date_start=None,
                         date_end=None,
                         lat_min=None,
                         lat_max=None,
                         lon_min=None,
                         lon_max=None,
                         scalar_filter=None,
                         ):
        md: Descriptor = self._cached.get(uuid)

        if md is not None:
            ds: ParquetDataQuery.Dataset = self._instance.get_dataset(md.key)
            return ds.get_data(date_start, date_end, lat_min, lat_max, lon_min, lon_max, scalar_filter)
        else:
            return None
