from data_access_service.models.bounding_box import BoundingBox
from dataclasses import dataclass


@dataclass(frozen=True)
class SubsetRequest:
    uuid: str
    keys: list[str]
    start_date: str
    end_date: str
    bboxes: [BoundingBox]
    recipient: str
    collection_title: str
    full_metadata_link: str
    suggested_citation: str
    # TODO: add more fields if needed
