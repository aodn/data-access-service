import geojson
import pytest
from shapely.geometry import Point as ShapelyPoint

from data_access_service.utils.multi_polygon_helper import (
    MultiPolygonHelper,
    merge_polygons,
    parse_multi_polygon,
)


def _multi_polygon(*rings) -> str:
    """A GeoJSON MultiPolygon string with one single-ring polygon per ring."""
    return geojson.dumps({"type": "MultiPolygon", "coordinates": [[r] for r in rings]})


def _rect(lon_min, lat_min, lon_max, lat_max) -> list:
    return [
        [lon_min, lat_min],
        [lon_max, lat_min],
        [lon_max, lat_max],
        [lon_min, lat_max],
        [lon_min, lat_min],
    ]


class TestParseMultiPolygon:
    def test_no_spatial_filter(self):
        assert parse_multi_polygon(None) is None
        assert parse_multi_polygon("non-specified") is None

    def test_string_dict_and_geojson_object_agree(self):
        as_string = _multi_polygon(_rect(10, 20, 30, 40))
        as_dict = geojson.loads(as_string)

        assert parse_multi_polygon(as_string).coordinates == as_dict.coordinates
        assert parse_multi_polygon(dict(as_dict)).coordinates == as_dict.coordinates
        assert parse_multi_polygon(as_dict) is as_dict

    def test_non_multipolygon_geometry_raises(self):
        with pytest.raises(TypeError):
            parse_multi_polygon('{"type": "Point", "coordinates": [10, 20]}')


class TestMergePolygons:
    def test_overlapping_polygons_dissolve_into_one(self):
        # two boxes sharing lon 20..30 -> one outline, overlap counted once
        merged = merge_polygons(
            _multi_polygon(_rect(10, 0, 30, 10), _rect(20, 0, 40, 10))
        )

        assert len(merged) == 1
        assert merged[0].bounds == (10, 0, 40, 10)
        # area of the union, NOT the sum of the two boxes (200 + 200 = 400)
        assert merged[0].area == pytest.approx(300)

    def test_disjoint_polygons_stay_apart(self):
        merged = merge_polygons(
            _multi_polygon(_rect(10, 0, 20, 10), _rect(50, 0, 60, 10))
        )

        assert len(merged) == 2
        assert sorted(polygon.bounds for polygon in merged) == [
            (10, 0, 20, 10),
            (50, 0, 60, 10),
        ]

    def test_touching_polygons_dissolve_into_one(self):
        merged = merge_polygons(
            _multi_polygon(_rect(10, 0, 20, 10), _rect(20, 0, 30, 10))
        )

        assert len(merged) == 1
        assert merged[0].bounds == (10, 0, 30, 10)

    def test_holes_are_kept(self):
        with_hole = geojson.dumps(
            {
                "type": "MultiPolygon",
                "coordinates": [[_rect(0, 0, 10, 10), _rect(2, 2, 4, 4)]],
            }
        )
        merged = merge_polygons(with_hole)

        assert len(merged) == 1
        assert merged[0].area == pytest.approx(100 - 4)
        assert not merged[0].contains(ShapelyPoint(3, 3))

    def test_no_spatial_filter_returns_empty(self):
        assert merge_polygons(None) == []
        assert merge_polygons("non-specified") == []
        assert merge_polygons('{"type": "MultiPolygon", "coordinates": []}') == []

    def test_zero_area_polygon_raises_instead_of_meaning_whole_globe(self):
        # a collapsed polygon leaves no area to subset. Returning [] would be
        # read as "no spatial filter" and download the whole globe.
        collapsed = _multi_polygon([[10, 20], [30, 20], [10, 20]])
        with pytest.raises(ValueError, match="encloses no area"):
            merge_polygons(collapsed)


class TestMultiPolygonHelper:
    def test_no_multi_polygon_means_no_bboxes(self):
        # All three views must agree that no spatial filter was given. Turning
        # that into a whole-globe bbox is ResolvedSubsetRequest.effective_bboxes'
        # job, not this parse layer's - defaulting here made .bboxes claim a
        # filter existed while .polygons/.geometry said it did not.
        helper = MultiPolygonHelper(multi_polygon=None)

        assert helper.bboxes == []
        assert helper.polygons == []
        assert helper.geometry is None

    def test_single_polygon_becomes_its_bbox(self):
        helper = MultiPolygonHelper(multi_polygon=_multi_polygon(_rect(10, 20, 30, 40)))

        assert len(helper.bboxes) == 1
        bbox = helper.bboxes[0]
        assert (bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) == (
            10,
            20,
            30,
            40,
        )

    def test_overlapping_polygons_give_one_bbox(self):
        helper = MultiPolygonHelper(
            multi_polygon=_multi_polygon(_rect(10, 0, 30, 10), _rect(20, 0, 40, 10))
        )

        assert len(helper.bboxes) == 1
        bbox = helper.bboxes[0]
        assert (bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) == (
            10,
            0,
            40,
            10,
        )

    def test_geometry_is_the_merged_shape_the_bboxes_came_from(self):
        # what the zarr subset masks with: one Polygon for one merged shape, a
        # MultiPolygon when the drawn areas stay apart, None when none was drawn
        one = MultiPolygonHelper(
            multi_polygon=_multi_polygon(_rect(10, 0, 30, 10), _rect(20, 0, 40, 10))
        )
        two = MultiPolygonHelper(
            multi_polygon=_multi_polygon(_rect(10, 0, 20, 10), _rect(50, 0, 60, 10))
        )

        assert one.geometry.geom_type == "Polygon"
        assert one.geometry.bounds == (10, 0, 40, 10)
        assert two.geometry.geom_type == "MultiPolygon"
        assert len(two.geometry.geoms) == 2
        assert MultiPolygonHelper(multi_polygon=None).geometry is None

    def test_disjoint_polygons_give_one_bbox_each_matching_polygons(self):
        helper = MultiPolygonHelper(
            multi_polygon=_multi_polygon(_rect(10, 0, 20, 10), _rect(50, 0, 60, 10))
        )

        assert len(helper.bboxes) == 2
        # bboxes and polygons stay in the same order, so the batch path can pair
        # a bbox with the shape it came from
        for bbox, polygon in zip(helper.bboxes, helper.polygons):
            assert polygon.bounds == (
                bbox.min_lon,
                bbox.min_lat,
                bbox.max_lon,
                bbox.max_lat,
            )
