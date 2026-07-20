from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.multi_polygon_helper import MultiPolygonHelper


def get_keys(parameters: dict) -> list[str]:
    """Parse the comma-separated `key` parameter; ['*'] when absent."""
    raw = parameters.get(Parameters.KEY.value)
    if raw is None:
        return ["*"]
    return [item.strip() for item in raw.split(",")]


def get_subset_request(parameters: dict) -> SubsetRequest:
    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]
    return SubsetRequest(
        uuid=parameters[Parameters.UUID.value],
        keys=get_keys(parameters),
        start_date=parameters[Parameters.START_DATE.value],
        end_date=parameters[Parameters.END_DATE.value],
        recipient=parameters[Parameters.RECIPIENT.value],
        multi_polygon=multi_polygon,
        bboxes=MultiPolygonHelper(multi_polygon=multi_polygon).bboxes,
        collection_title=parameters.get(Parameters.COLLECTION_TITLE.value),
        full_metadata_link=parameters.get(Parameters.FULL_METADATA_LINK.value),
        suggested_citation=parameters.get(Parameters.SUGGESTED_CITATION.value),
        output_format=parameters.get(Parameters.OUTPUT_FORMAT.value),
    )
