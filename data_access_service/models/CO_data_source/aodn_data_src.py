from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource

from data_access_service.models.CO_data_source.abstract_data_src import AbstractDataSrc


class AodnDataSrc(AbstractDataSrc):

    def __init__(self):
        pass

    def get_metadata(self) -> Metadata:
        pass

    def get_metadata_catalog(self) -> dict:
        pass

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        pass
