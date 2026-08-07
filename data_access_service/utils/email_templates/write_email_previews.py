"""Render every email template to an HTML file for browser preview."""

from pathlib import Path

from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)
from data_access_service.utils.email_templates.no_data_email import (
    get_no_data_email_html_body,
)


def write_email_previews(
    subset_request: SubsetRequest, object_urls: list[str], output_dir: Path
) -> list[Path]:
    """Write one preview HTML file per email template; returns the file paths."""
    previews = {
        "preview_email.html": get_download_email_html_body(subset_request, object_urls),
        "preview_no_data_email.html": get_no_data_email_html_body(subset_request),
    }

    paths = []
    for file_name, html in previews.items():
        path = output_dir / file_name
        path.write_text(html, encoding="utf-8")
        paths.append(path)
    return paths
