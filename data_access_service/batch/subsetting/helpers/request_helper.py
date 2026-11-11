import json
import logging
from typing import Optional

from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.multi_polygon_helper import MultiPolygonHelper
from data_access_service.utils.subset_request_resolver import parse_keys

logger = logging.getLogger(__name__)


def get_subset_request(parameters: dict) -> SubsetRequest:
    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]
    return SubsetRequest(
        uuid=parameters[Parameters.UUID.value],
        keys=parse_keys(parameters.get(Parameters.KEY.value)),
        start_date=parameters[Parameters.START_DATE.value],
        end_date=parameters[Parameters.END_DATE.value],
        recipient=parameters[Parameters.RECIPIENT.value],
        multi_polygon=parameters[Parameters.MULTI_POLYGON.value],
        bboxes=MultiPolygonHelper(multi_polygon=multi_polygon).bboxes,
        collection_title=parameters.get(Parameters.COLLECTION_TITLE.value),
        # Written by init as "true"/"false"; absent when the job predates it.
        collection_has_multi_datasets=(
            parameters.get(Parameters.COLLECTION_HAS_MULTI_DATASETS.value) == "true"
        ),
        full_metadata_link=parameters.get(Parameters.FULL_METADATA_LINK.value),
        suggested_citation=parameters.get(Parameters.SUGGESTED_CITATION.value),
        usage_constraints=parse_usage_constraints(
            parameters.get(Parameters.USAGE_CONSTRAINTS.value)
        ),
        output_format=parameters.get(Parameters.OUTPUT_FORMAT.value),
    )


def parse_usage_constraints(value: Optional[str]) -> list[str]:
    """Read the JSON list ogcapi sends, e.g. '["A", "B"]' or '[]'.

    [] when it is missing or not a JSON list of strings, so the email says
    "not available" instead of failing the job.
    """
    if value is None:
        return []
    try:
        constraints = json.loads(value)
    except (TypeError, ValueError):
        constraints = None
    if not isinstance(constraints, list) or not all(
        isinstance(item, str) for item in constraints
    ):
        logger.warning("Ignoring invalid usage_constraints: %r", value)
        return []
    return constraints
