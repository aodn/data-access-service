from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from data_access_service.models.bounding_box import BoundingBox

# Marker value for an optional field the user did not provide (dates,
# multi_polygon, ...). AWS Batch job parameters are plain strings and cannot
# carry None, so absent values travel as this literal instead.
NON_SPECIFIED = "non-specified"
SUPPORTED_OUTPUT_FORMATS = frozenset({"netcdf", "geotiff", "csv"})


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
    start_date: str  # "YYYY-MM-DD" / "MM-YYYY" / NON_SPECIFIED to use default
    end_date: str  # same formats; NON_SPECIFIED to use default

    # --- where to send the result ---
    recipient: str  # email address

    # --- output preferences ---
    output_format: str  # one of SUPPORTED_OUTPUT_FORMATS

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

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.uuid:
            raise ValueError("uuid must be a non-empty string")
        if not self.keys:
            raise ValueError("keys must be a non-empty list")
        if "@" not in (self.recipient or ""):
            raise ValueError(
                f"recipient must be a valid email address, got {self.recipient!r}"
            )
        if self.output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"output_format must be one of {sorted(SUPPORTED_OUTPUT_FORMATS)}, "
                f"got {self.output_format!r}"
            )

        start = self._parse_date_or_default(self.start_date, field_name="start_date")
        end = self._parse_date_or_default(self.end_date, field_name="end_date")
        if start is not None and end is not None and start > end:
            raise ValueError(
                f"start_date ({self.start_date}) must be on or before "
                f"end_date ({self.end_date})"
            )

    @staticmethod
    def _parse_date_or_default(
        value: str, *, field_name: str
    ) -> Optional[pd.Timestamp]:
        if value == NON_SPECIFIED:
            return None
        try:
            return pd.Timestamp(value)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"{field_name} must be a parseable date or "
                f"{NON_SPECIFIED!r}, got {value!r}"
            ) from exc
