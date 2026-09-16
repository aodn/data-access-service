from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class CsiroConfig:
    collection_url: str
    key_request_url: str
    # Folder CSIRO puts the parquet in, inside the collection folder.
    data_folder: str
    request_timeout_seconds: int
    # One entry per dataset, each with a 'dataset_name' and a 'fedora_pid'.
    datasets: List[Dict]
