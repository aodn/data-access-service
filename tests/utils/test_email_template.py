"""Tests for the download email's subsetting section: classifying a selection
into bounding boxes vs freeform polygons and rendering it (plus the time range)
into the email HTML."""

import unittest
import pandas
from data_access_service.utils.email_templates.subsetting_geometry import (
    split_bboxes_and_polygons,
)
from data_access_service.utils.email_templates.form_polygon_divs import (
    form_polygon_divs,
)
from data_access_service.utils.email_templates.form_bbox_divs import (
    form_bbox_divs,
)
from data_access_service.utils.email_templates.form_subsetting_divs import (
    form_subsetting_divs,
)
from data_access_service.utils.email_templates.coordinate_format import (
    format_coordinate,
)
from data_access_service.models.bounding_box import BoundingBox


# ---------------------------------------------------------------------------
# The user draws an area on the map; the email shows it back to them.
#
# The area comes in as GeoJSON, and we decide how to display it by counting its
# corners. GeoJSON repeats the first corner at the end to "close" the shape, so
# that duplicate is not counted:
#     4 corners or fewer -> a rectangle, shown as a "Bounding Box"
#     more than 4 corners -> shown as a "Polygon"
#
# Note: GeoJSON lists each corner as [longitude, latitude], but the email shows
# it the usual way round, latitude first.
# ---------------------------------------------------------------------------

# Pentagon: 5 unique vertices (+ closing point) -> a Polygon
FREEFORM_POLYGON = (
    '{"type":"MultiPolygon","coordinates":[[[[145.0,-40.0],[146.0,-40.0],'
    "[146.5,-41.0],[145.5,-42.0],[144.5,-41.0],[145.0,-40.0]]]]}"
)

# Rectangle: 4 unique vertices (+ closing point) -> a Bounding Box
RECTANGLE_BBOX = (
    '{"type":"MultiPolygon","coordinates":[[[[145,-40],[145,-41],'
    "[146,-41],[146,-40],[145,-40]]]]}"
)

# One request holding both a rectangle ring and a pentagon ring
MIXED_GEOMETRY = (
    '{"type":"MultiPolygon","coordinates":['
    "[[[145,-40],[145,-41],[146,-41],[146,-40],[145,-40]]],"
    "[[[145.0,-40.0],[146.0,-40.0],[146.5,-41.0],[145.5,-42.0],[144.5,-41.0],[145.0,-40.0]]]"
    "]}"
)


class TestSubsettingPolygon(unittest.TestCase):

    def test_split_no_area_returns_empty(self):
        # No area selected (None or the "non-specified" sentinel) -> nothing spatial.
        self.assertEqual(split_bboxes_and_polygons(None), ([], []))
        self.assertEqual(split_bboxes_and_polygons("non-specified"), ([], []))

    def test_split_freeform_is_polygon_not_bbox(self):
        # 5 unique vertices (> 4) -> a freeform polygon, not a bounding box.
        bboxes, polygons = split_bboxes_and_polygons(FREEFORM_POLYGON)
        self.assertEqual(len(bboxes), 0)
        self.assertEqual(len(polygons), 1)
        self.assertEqual(
            len(polygons[0]), 5
        )  # 6 GeoJSON points minus the closing point

    def test_split_rectangle_is_bbox_not_polygon(self):
        # 4 unique vertices (<= 4) -> collapsed into a bounding box, not a polygon.
        bboxes, polygons = split_bboxes_and_polygons(RECTANGLE_BBOX)
        self.assertEqual(len(bboxes), 1)
        self.assertEqual(len(polygons), 0)

    def test_form_polygon_divs_lists_vertices(self):
        _, polygons = split_bboxes_and_polygons(FREEFORM_POLYGON)
        html = form_polygon_divs(polygons)

        self.assertIn("Polygon Selection", html)
        self.assertIn("Point 1: (-40.00000, 145.00000)", html)  # latitude-first display
        self.assertIn("Point 5: (-41.00000, 144.50000)", html)
        self.assertIn("data:image/png;base64,", html)  # polygon icon embedded

    def test_freeform_polygon_shows_polygon_section(self):
        html = form_subsetting_divs("non-specified", "non-specified", FREEFORM_POLYGON)

        self.assertIn("Polygon Selection", html)
        self.assertNotIn("Bounding Box", html)

    def test_rectangle_shows_bbox_not_polygon(self):
        html = form_subsetting_divs("non-specified", "non-specified", RECTANGLE_BBOX)

        self.assertIn("Bounding Box Selection", html)
        self.assertNotIn("Polygon Selection", html)

    def test_split_mixed_geometry_separates_bbox_and_polygon(self):
        # A MultiPolygon can carry both kinds at once; each ring is classified
        # independently rather than the whole request being one or the other.
        bboxes, polygons = split_bboxes_and_polygons(MIXED_GEOMETRY)
        self.assertEqual(len(bboxes), 1)
        self.assertEqual(len(polygons), 1)
        self.assertEqual(len(polygons[0]), 5)  # closing point removed

    def test_mixed_geometry_shows_both_sections(self):
        html = form_subsetting_divs("non-specified", "non-specified", MIXED_GEOMETRY)

        self.assertIn("Bounding Box Selection", html)
        self.assertIn("Polygon Selection", html)

    def test_default_dates_are_suppressed(self):
        # The portal's "no time filter" default is 1970-01-01 .. today; it must
        # not render as a time range. Pair it with a polygon so the section still
        # renders and we can assert the time range specifically is absent.
        today = pandas.Timestamp.today().strftime("%Y-%m-%d")
        html = form_subsetting_divs("1970-01-01", today, FREEFORM_POLYGON)

        self.assertIn("Polygon Selection", html)
        self.assertNotIn("Time Range", html)

    def test_default_dates_without_area_returns_empty(self):
        # Default dates and no spatial selection means nothing to show at all.
        today = pandas.Timestamp.today().strftime("%Y-%m-%d")
        self.assertEqual(form_subsetting_divs("1970-01-01", today, None), "")

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

    def test_format_coordinate_rounds_to_five_decimals(self):
        self.assertEqual(format_coordinate(-35.123456789), "-35.12346")
        self.assertEqual(format_coordinate(145.0), "145.00000")

    def test_format_coordinate_avoids_scientific_notation(self):
        self.assertEqual(format_coordinate(4.9e-324), "0.00000")
        self.assertEqual(format_coordinate(-11.919807423710694), "-11.91981")

    def test_bbox_divs_round_coordinates(self):
        bbox = BoundingBox(
            min_lon=145.123456789,
            min_lat=-41.0,
            max_lon=146.0,
            max_lat=-40.987654321,
        )
        html = form_bbox_divs([bbox])

        self.assertIn("W: 145.12346", html)
        self.assertIn("N: -40.98765", html)


if __name__ == "__main__":
    unittest.main()
