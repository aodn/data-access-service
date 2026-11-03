"""Tests for the shared email body: every email renders the same sections
(collection, data selection, format, metadata, citation, usage constraints);
only the intro under "Hi," and the download button differ."""

import re
import unittest
from html import unescape
from pathlib import Path
from tempfile import TemporaryDirectory

from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)
from data_access_service.utils.email_templates.no_data_email import (
    get_no_data_email_html_body,
)
from data_access_service.utils.email_templates.error_email import (
    get_error_email_html_body,
)
from data_access_service.utils.email_templates.write_email_previews import (
    write_email_previews,
)

# Rectangle: 4 unique vertices (+ closing point) -> a Bounding Box
RECTANGLE_BBOX = (
    '{"type":"MultiPolygon","coordinates":[[[[145,-40],[145,-41],'
    "[146,-41],[146,-40],[145,-40]]]]}"
)


def visible_to_email_clients(html: str) -> str:
    """What a non-Outlook client renders: HTML comments (including the
    Outlook-conditional ones) are stripped."""
    return re.sub(r"<!--.*?-->", "", html, flags=re.S)


class TestEmailBodies(unittest.TestCase):

    def test_error_email_has_requested_intro_and_shared_sections(self):
        request = SubsetRequest(
            uuid="test-uuid-1234",
            keys=["slocum_glider_delayed_qc"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="csv",
            multi_polygon=RECTANGLE_BBOX,
            collection_title="Test Collection Title",
            full_metadata_link="https://example.com/metadata",
            suggested_citation="Suggested citation goes here.",
        )
        html = visible_to_email_clients(get_error_email_html_body(request))
        text = " ".join(unescape(re.sub(r"<[^>]+>", " ", html)).split())
        self.assertIn(
            "Hi, Unfortunately, an error occurred while processing your download request. "
            "Please try again. If the problem persists, please contact us at "
            "info@aodn.org.au for assistance.",
            text,
        )
        self.assertIn('href="mailto:info@aodn.org.au"', html)
        for content in (
            "Collection",
            "Test Collection Title",
            "test-uuid-1234",
            "Data Selection",
            "slocum_glider_delayed_qc",
            "Format",
            "CSV",
            "Bounding Box Selection",
            "01 Jan 2020 00:00:00 UTC - 30 Jun 2020 23:59:59 UTC",
            "Metadata",
            "https://example.com/metadata",
            "Suggested Citation",
            "Suggested citation goes here.",
            "Usage Constraints",
            "Kind regards,",
        ):
            self.assertIn(content, html)
        self.assertNotIn("Download</span>", html)
        self.assertNotIn("download request has been completed", html)
        self.assertNotIn("The download will be available for 7 days.", html)
        self.assertNotIn("No data available", html)

    def test_preview_helper_includes_error_email(self):
        request = SubsetRequest(
            uuid="test-uuid",
            keys=["dataset"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="csv",
            multi_polygon=None,
        )
        with TemporaryDirectory() as directory:
            paths = write_email_previews(
                request, ["https://example.com/result"], Path(directory)
            )
            self.assertEqual(
                {path.name for path in paths},
                {
                    "preview_email.html",
                    "preview_no_data_email.html",
                    "preview_error_email.html",
                },
            )
            self.assertEqual(
                (Path(directory) / "preview_error_email.html").read_text(
                    encoding="utf-8"
                ),
                get_error_email_html_body(request),
            )

    def test_emails_have_no_hardcoded_mooring_constraints(self):
        """Guard #9241 for completion formats and the shared no-data body."""
        for output_format in ("csv", "netcdf", "geotiff"):
            request = SubsetRequest(
                uuid="test-uuid",
                keys=["test-dataset"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                recipient="test@example.com",
                output_format=output_format,
                multi_polygon=None,
                full_metadata_link="https://example.com/metadata",
                suggested_citation="Cite this dataset as X.",
            )
            bodies = {
                "completed": get_download_email_html_body(
                    request, ["https://example.com/result"]
                ),
                "no_data": get_no_data_email_html_body(request),
                "error": get_error_email_html_body(request),
                "empty_urls": get_download_email_html_body(request, []),
            }
            for kind, html in bodies.items():
                with self.subTest(output_format=output_format, kind=kind):
                    for marker in (
                        "TAN100",
                        "ORS065",
                        "Ningaloo",
                        "Sydney Water Corporation",
                        "If using data from",
                    ):
                        self.assertNotIn(marker, html)
                    self.assertIn(
                        "Any users of IMOS data are required to clearly acknowledge",
                        html,
                    )
                    self.assertIn(
                        'Data, products and services from IMOS are provided "as is" '
                        "without any warranty",
                        html,
                    )
                    self.assertIn("Cite this dataset as X.", html)
                    self.assertIn("https://example.com/metadata", html)
                    if kind == "completed":
                        self.assertIn("https://example.com/result", html)

    def test_no_data_email_contains_all_shared_sections(self):
        request = SubsetRequest(
            uuid="test-uuid-1234",
            keys=["slocum_glider_delayed_qc"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="csv",
            multi_polygon=RECTANGLE_BBOX,
            collection_title="Test Collection Title",
            full_metadata_link="https://example.com/metadata",
            suggested_citation="Suggested citation goes here.",
        )

        html = visible_to_email_clients(get_no_data_email_html_body(request))

        self.assertIn(
            "No data available for the requested subset of the dataset collection.",
            html,
        )
        self.assertIn("Collection", html)
        self.assertIn("Test Collection Title", html)
        self.assertIn("test-uuid-1234", html)
        self.assertIn("Data Selection", html)
        self.assertIn("slocum_glider_delayed_qc", html)
        self.assertIn("Format", html)
        self.assertIn("CSV", html)
        self.assertIn("Bounding Box Selection", html)
        self.assertIn("01 Jan 2020 00:00:00 UTC - 30 Jun 2020 23:59:59 UTC", html)
        self.assertIn("Metadata", html)
        self.assertIn("https://example.com/metadata", html)
        self.assertIn("Suggested Citation", html)
        self.assertIn("Suggested citation goes here.", html)
        self.assertIn("Usage Constraints", html)
        self.assertIn("Any users of IMOS data", html)
        self.assertIn("Kind regards,", html)
        # no download parts in the no-data email
        self.assertNotIn("Download</span>", html)
        self.assertNotIn("download request has been completed", html)

    def test_download_email_has_link_button_and_shared_sections(self):
        request = SubsetRequest(
            uuid="test-uuid-1234",
            keys=["slocum_glider_delayed_qc"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="netcdf",
            multi_polygon=RECTANGLE_BBOX,
            collection_title="Test Collection Title",
            full_metadata_link="https://example.com/metadata",
            suggested_citation="Suggested citation goes here.",
        )

        html = visible_to_email_clients(
            get_download_email_html_body(request, ["https://example.com/result.nc"])
        )

        self.assertIn("download request has been completed", html)
        self.assertIn("https://example.com/result.nc", html)
        self.assertIn("Download</span>", html)
        self.assertIn("The download will be available for 7 days.", html)
        # same shared body as the no-data email
        self.assertIn("Test Collection Title", html)
        self.assertIn("test-uuid-1234", html)
        self.assertIn("Data Selection", html)
        self.assertIn("NETCDF", html)
        self.assertIn("Usage Constraints", html)

    def test_download_email_multiple_urls_hides_button_keeps_links(self):
        request = SubsetRequest(
            uuid="test-uuid-1234",
            keys=["a.parquet", "b.parquet"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="csv",
            multi_polygon=None,
        )

        html = visible_to_email_clients(
            get_download_email_html_body(
                request, ["https://test/a.zip", "https://test/b.zip"]
            )
        )

        self.assertIn("https://test/a.zip", html)
        self.assertIn("https://test/b.zip", html)
        self.assertNotIn("Download</span>", html)

    def test_download_email_without_urls_falls_back_to_no_data_email(self):
        request = SubsetRequest(
            uuid="test-uuid-1234",
            keys=["a.parquet"],
            start_date="2020-01-01",
            end_date="2020-06-30",
            recipient="test@example.com",
            output_format="csv",
            multi_polygon=None,
        )

        self.assertEqual(
            get_download_email_html_body(request, []),
            get_no_data_email_html_body(request),
        )


if __name__ == "__main__":
    unittest.main()
