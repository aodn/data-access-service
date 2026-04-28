from dataclasses import dataclass, field
from typing import Optional

from data_access_service.models.bounding_box import BoundingBox


@dataclass(frozen=True)
class SubsetRequest:
    """A user's subset request.

    Input:  dataset, time range, area, recipient, output format.
    Output: one object holding all of the above together.
    """

    # --- which data ---
    uuid: str  # AODN dataset UUID
    keys: list[str]  # file names within the dataset; ["*"] means "all files"

    # --- time window ---
    start_date: str  # "YYYY-MM-DD" / "MM-YYYY" / "non-specified" for open-start
    end_date: str  # same formats; "non-specified" for open-end

    # --- where to send the result ---
    recipient: str  # email address

    # --- spatial filter (optional) ---
    multi_polygon: Optional[str] = (
        None  # GeoJSON MultiPolygon string; None = no spatial filter
    )
    bboxes: list[BoundingBox] = field(
        default_factory=list
    )  # parsed from multi_polygon by get_subset_request

    # --- email metadata (optional, shown in the result email) ---
    collection_title: Optional[str] = None
    full_metadata_link: Optional[str] = None
    suggested_citation: Optional[str] = None

    # --- output preferences ---
    output_format: str = "netcdf"  # "netcdf" or "geotiff"
