import logging
import threading
from abc import ABC
from datetime import timedelta

from aodn_cloud_optimised.lib.DataQuery import (
    BUCKET_OPTIMISED_DEFAULT,
    Metadata,
    DataSource,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.co_datasource.abstract_data_src import (
    AbstractDataSrc,
    AODN,
)
from data_access_service.models.co_datasource.aodn_data_src import AodnDataSrc
from data_access_service.models.co_datasource.csiro.csiro_data_src import CsiroDataSrc
from data_access_service.models.co_datasource.dataset_location import DatasetLocation
from data_access_service.utils.common_utils import compare_dict_keys
from data_access_service.utils.retry_utils import log_retry_attempt

log = logging.getLogger(__name__)

# Every cloud optimised data source, in the order they are asked where a
# dataset lives. Adding a provider means adding it here *and* to the list
# CODataRegistry builds below.
#
# The classes land in this list at import time, so the autouse mock in
# tests/conftest.py (which patches the CsiroDataSrc name in this module) does
# not reach them: a test that resolves a CSIRO dataset must patch
# csiro_data_src.requests.get, or this list, itself.
_DATA_SOURCES: list[type[AbstractDataSrc]] = [AodnDataSrc, CsiroDataSrc]


def resolve_dataset_location(dataset_name: str) -> DatasetLocation:
    """Where ``dataset_name`` lives, falling back to the AODN bucket.

    Asks the classes, not a :class:`CODataRegistry`, because the batch jobs
    call this from forked children: building a registry there would reload the
    whole catalog and request keys for every dataset, to answer about one.
    """
    for source in _DATA_SOURCES:
        location = source.locate_dataset(dataset_name)
        if location is not None:
            return location
    return DatasetLocation(bucket=BUCKET_OPTIMISED_DEFAULT)


class CODataRegistry(ABC):

    # The wait grows in minutes -> 5, 5, ... 5, 10, 20, 30
    GET_DATASET_MIN_WAIT = timedelta(minutes=5)
    GET_DATASET_MAX_WAIT = timedelta(minutes=30)
    GET_DATASET_MAX_ATTEMPTS = 11

    def __init__(self):
        log.info("Initializing all Cloud Optimized data sources...")
        self.data_source_list: list[AbstractDataSrc] = [AodnDataSrc(), CsiroDataSrc()]
        # ParquetDataSource.dataset lists the hive tree on first access
        # (argo.parquet is ~300k files). One instance per name so
        # get_temporal_extent and get_datasource share that listing.
        self._datasets: dict[str, DataSource] = {}
        self._datasets_lock = threading.Lock()
        log.info("All Cloud Optimized data sources initialized")

    # since only catalog in DataQuery.Metadata is using by this project now, so only combine the catalogs for now.
    def get_metadata(self) -> Metadata:
        log.info("Getting metadata from all data sources...")

        # Just a temp solution for now since the GetAodn.get_metadata() doesn't work for external data source
        aodn = next(src for src in self.data_source_list if src.get_name() == AODN)
        log.info("Getting metadata from AODN data source...")
        metadata = aodn.get_metadata()

        log.info("Getting metadata from external data sources ...")
        for src in self.data_source_list:
            if src.get_name() == AODN:
                continue

            log.info(f"Getting metadata catalog from data source {src.get_name()}...")
            catalog = src.get_metadata_catalog()
            has_same_dataset_name, conflicted_names = compare_dict_keys(
                metadata.catalog, catalog
            )
            if has_same_dataset_name:
                raise Exception(
                    f"Conflicted dataset names found in different data sources: {conflicted_names}. Please contact Data Uplift team"
                )
            metadata.catalog = metadata.catalog | catalog

        log.info("Metadata retrieved from all data source")
        return metadata

    # Bug in tenacity, the type check always fail but function ok
    # noinspection PyCallingNonCallable
    @retry(
        stop=stop_after_attempt(GET_DATASET_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=1, min=GET_DATASET_MIN_WAIT, max=GET_DATASET_MAX_WAIT
        ),
        retry=retry_if_exception_type(ValueError),
        before_sleep=log_retry_attempt("get_dataset", log),
        reraise=True,
    )
    def get_dataset(self, dataset_name_with_ext: str) -> DataSource | None:
        with self._datasets_lock:
            cached = self._datasets.get(dataset_name_with_ext)
            if cached is not None:
                log.info("Reusing cached %s dataset", dataset_name_with_ext)
                return cached

        for data_src in self.data_source_list:
            try:
                log.info(
                    f"Getting {dataset_name_with_ext} dataset from {data_src.get_name()}..."
                )
                dataset = data_src.get_dataset(dataset_name_with_ext)
            except DatasetNotFoundError:
                # log the exception and continue to try the next data source
                log.info(
                    f"Dataset {dataset_name_with_ext} not found in data source {data_src}. Trying next data source"
                )
                continue
            # Load happened outside the lock so a 4-minute S3 listing does
            # not block other datasets. setdefault keeps the first stored
            # instance if two threads both missed.
            with self._datasets_lock:
                return self._datasets.setdefault(dataset_name_with_ext, dataset)

        raise Exception(f"Dataset {dataset_name_with_ext} not found in any data source")
