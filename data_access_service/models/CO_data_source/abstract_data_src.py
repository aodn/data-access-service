from abc import ABC, abstractmethod

from aodn_cloud_optimised.lib.DataQuery import DataSource, Metadata


class AbstractDataSrc(ABC):

    @abstractmethod
    def get_metadata(self) -> Metadata:
        pass

    @abstractmethod
    def get_metadata_catalog(self) -> dict:
        pass

    @abstractmethod
    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        pass
