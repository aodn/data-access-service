import unittest
from data_access_service.utils.email_generator import (
    generate_started_email_subject,
    generate_completed_email_subject,
    generate_started_email_content,
    generate_completed_email_content,
)
from data_access_service.utils.email_templates.subsetting_geometry import (
    split_bboxes_and_polygons,
)
from data_access_service.utils.email_templates.form_polygon_divs import (
    form_polygon_divs,
)
from data_access_service.utils.email_templates.form_subsetting_divs import (
    form_subsetting_divs,
)


class TestEmailGenerator(unittest.TestCase):

    def test_generate_started_email_subject(self):
        uuid = "12345"
        expected_subject = "start processing data file whose uuid is: 12345"
        self.assertEqual(generate_started_email_subject(uuid), expected_subject)

    def test_generate_completed_email_subject(self):
        uuid = "12345"
        expected_subject = "finish processing data file whose uuid is: 12345"
        self.assertEqual(generate_completed_email_subject(uuid), expected_subject)

    def test_generate_started_email_content(self):
        uuid = "12345"
        conditions = [("start date", "2023-01-01"), ("end date", "2023-01-31")]
        expected_content = (
            "already start processing12345. \nThe condition(s): "
            "start date: 2023-01-01,\n end date: 2023-01-31,\n "
            ". \nPlease wait for the result. After the process is done, you will receive another email."
        )
        self.assertEqual(
            generate_started_email_content(uuid, conditions), expected_content
        )

    def test_generate_completed_email_content(self):
        uuid = "12345"
        conditions = [("start date", "2023-01-01"), ("end date", "2023-01-31")]
        object_url = "http://example.com/download"
        expected_content = (
            "The result is ready. \nThe id of the dataset is: 12345"
            "The condition(s): start date: 2023-01-01, end date: 2023-01-31, "
            ". \nYou can download it. The download link is: http://example.com/download"
        )
        self.assertEqual(
            generate_completed_email_content(uuid, conditions, object_url),
            expected_content,
        )


# ---------------------------------------------------------------------------
# Polygon subsetting in the download email
# (bbox vs freeform polygon split + polygon rendering)
# ---------------------------------------------------------------------------

# A freeform polygon: a pentagon (5 unique vertices + closing point)
FREEFORM_POLYGON = (
    '{"type":"MultiPolygon","coordinates":[[[[145.0,-40.0],[146.0,-40.0],'
    "[146.5,-41.0],[145.5,-42.0],[144.5,-41.0],[145.0,-40.0]]]]}"
)

# A rectangle bounding box (4 unique vertices + closing point)
RECTANGLE_BBOX = (
    '{"type":"MultiPolygon","coordinates":[[[[145,-40],[145,-41],'
    "[146,-41],[146,-40],[145,-40]]]]}"
)


class TestSubsettingPolygon(unittest.TestCase):

    def test_split_non_specified_returns_empty(self):
        self.assertEqual(split_bboxes_and_polygons(None), ([], []))
        self.assertEqual(split_bboxes_and_polygons("non-specified"), ([], []))

    def test_split_freeform_is_polygon_not_bbox(self):
        bboxes, polygons = split_bboxes_and_polygons(FREEFORM_POLYGON)
        self.assertEqual(len(bboxes), 0)
        self.assertEqual(len(polygons), 1)
        self.assertEqual(len(polygons[0]), 5)  # closing point removed

    def test_split_rectangle_is_bbox_not_polygon(self):
        bboxes, polygons = split_bboxes_and_polygons(RECTANGLE_BBOX)
        self.assertEqual(len(bboxes), 1)
        self.assertEqual(len(polygons), 0)

    def test_form_polygon_divs_lists_vertices(self):
        _, polygons = split_bboxes_and_polygons(FREEFORM_POLYGON)
        html = form_polygon_divs(polygons)

        self.assertIn("Polygon Selection", html)
        self.assertIn("Point 1: (-40.0, 145.0)", html)  # latitude-first display
        self.assertIn("Point 5: (-41.0, 144.5)", html)
        self.assertIn("data:image/png;base64,", html)  # polygon icon embedded

    def test_freeform_polygon_shows_polygon_section(self):
        html = form_subsetting_divs("non-specified", "non-specified", FREEFORM_POLYGON)

        self.assertIn("Polygon Selection", html)
        self.assertNotIn("Bounding Box", html)

    def test_rectangle_shows_bbox_not_polygon(self):
        html = form_subsetting_divs("non-specified", "non-specified", RECTANGLE_BBOX)

        self.assertIn("Bounding Box Selection", html)
        self.assertNotIn("Polygon Selection", html)

    def test_time_range_uses_australian_date_format(self):
        html = form_subsetting_divs("2024-01-05", "2024-12-31", None)

        self.assertIn("05/01/2024 - 31/12/2024", html)

    def test_malformed_geometry_does_not_crash(self):
        # A malformed multi_polygon must not break the email; the spatial section
        # is simply omitted while the rest of the email still renders.
        html = form_subsetting_divs("2024-01-05", "2024-12-31", "not-valid-geojson")

        self.assertIn("05/01/2024 - 31/12/2024", html)
        self.assertNotIn("Bounding Box", html)
        self.assertNotIn("Polygon Selection", html)


if __name__ == "__main__":
    unittest.main()
