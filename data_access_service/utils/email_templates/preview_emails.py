"""Render the email templates to HTML files so you can preview them in a
browser. No AWS/SES needed - it just calls the template functions directly.

Run:  poetry run python data_access_service/utils/email_templates/preview_emails.py
      (writes preview_email.html / preview_no_data_email.html to the current directory)
Edit the SubsetRequest below to try different areas / dates / metadata.
"""

import webbrowser
from pathlib import Path

from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.write_email_previews import (
    write_email_previews,
)

# One ring of each kind, so the preview shows every layout at once:
# two rectangles -> Bounding Box Selection, a triangle and a pentagon -> Polygon Selection.
SAMPLE_MULTI_POLYGON = (
    '{"type":"MultiPolygon","coordinates":['
    "[[[145.0,-40.0],[145.0,-41.0],[146.0,-41.0],[146.0,-40.0],[145.0,-40.0]]],"  # bounding box 1
    "[[[150.0,-35.0],[150.0,-33.0],[152.0,-33.0],[152.0,-35.0],[150.0,-35.0]]],"  # bounding box 2
    "[[[145.0,-40.0],[146.0,-41.0],[144.5,-41.5],[145.0,-40.0]]],"  # 3-point polygon (triangle)
    "[[[145.0,-40.0],[146.0,-40.0],[146.5,-41.0],[145.5,-42.0],[144.5,-41.0],[145.0,-40.0]]]"  # 5-point polygon (pentagon)
    "]}"
)

request = SubsetRequest(
    uuid="test-uuid-1234",
    keys=["*"],
    start_date="2020-01-01",
    end_date="2020-06-30",
    recipient="test@example.com",
    output_format="netcdf",
    multi_polygon=SAMPLE_MULTI_POLYGON,
    collection_title="Test Collection Title",
    full_metadata_link="https://example.com/metadata",
    suggested_citation="Suggested citation goes here.",
)

for path in write_email_previews(
    request, ["https://example.com/download/result.nc"], Path.cwd()
):
    print(f"Wrote {path}")
    webbrowser.open(path.as_uri())
