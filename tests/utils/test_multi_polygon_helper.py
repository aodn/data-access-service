import geojson
import pytest
from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon

from data_access_service.utils.multi_polygon_helper import (
    MultiPolygonHelper,
    merge_polygons,
    parse_multi_polygon,
    split_at_dateline,
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


class TestSplitAtDateline:
    def test_already_in_range_is_unchanged(self):
        poly = ShapelyPolygon(_rect(10, 0, 20, 10))
        pieces = split_at_dateline(poly)

        assert len(pieces) == 1
        assert pieces[0].equals(poly)

    def test_mapbox_unwrapped_rectangle_splits_at_dateline(self):
        # Portal/Mapbox example: continuous box with lon < -180 that crosses
        # the antimeridian. Must become two boxes that meet at ±180, not a
        # single bogus strip from wrapping vertices alone.
        lon_min, lon_max = -212.67488435641027, -157.75735934215612
        lat_min, lat_max = -39.90820956224461, -9.159204863042774
        poly = ShapelyPolygon(_rect(lon_min, lat_min, lon_max, lat_max))

        pieces = split_at_dateline(poly)
        bounds = sorted(p.bounds for p in pieces)

        assert len(pieces) == 2
        west = (lon_min + 360, lat_min, 180.0, lat_max)
        east = (-180.0, lat_min, lon_max, lat_max)
        assert bounds[0] == pytest.approx(east)
        assert bounds[1] == pytest.approx(west)
        assert sum(p.area for p in pieces) == pytest.approx(poly.area)
        for piece in pieces:
            min_lon, _, max_lon, _ = piece.bounds
            assert -180.0 <= min_lon <= max_lon <= 180.0

    def test_lon_above_180_splits_at_dateline(self):
        poly = ShapelyPolygon(_rect(170, -10, 200, 10))
        pieces = split_at_dateline(poly)
        bounds = sorted(p.bounds for p in pieces)

        assert len(pieces) == 2
        assert bounds[0] == pytest.approx((-180.0, -10, -160.0, 10))
        assert bounds[1] == pytest.approx((170.0, -10, 180.0, 10))

    def test_freeform_unwrapped_crossing_dateline(self):
        # Triangle with one vertex west of the dateline in unwrapped space.
        ring = [
            [-200.0, 0.0],
            [-160.0, 0.0],
            [-180.0, 20.0],
            [-200.0, 0.0],
        ]
        poly = ShapelyPolygon(ring)
        pieces = split_at_dateline(poly)

        assert len(pieces) >= 2
        assert sum(p.area for p in pieces) == pytest.approx(poly.area)
        # A point known inside the original on each side of -180.
        west_point = ShapelyPoint(-190.0, 2.0)  # unwraps to lon 170
        east_point = ShapelyPoint(-170.0, 2.0)
        assert poly.contains(west_point) and poly.contains(east_point)
        covered = ShapelyPolygon()
        for piece in pieces:
            min_lon, _, max_lon, _ = piece.bounds
            assert -180.0 <= min_lon <= max_lon <= 180.0
            covered = covered.union(piece)
        # After wrap: west_point becomes (170, 2); east stays (-170, 2).
        assert any(p.contains(ShapelyPoint(170.0, 2.0)) for p in pieces)
        assert any(p.contains(ShapelyPoint(-170.0, 2.0)) for p in pieces)

    def test_hole_on_one_side_is_kept(self):
        outer = _rect(-200, 0, -160, 20)
        # Hole entirely east of the dateline in unwrapped space (lon > -180).
        hole = _rect(-175, 5, -165, 15)
        poly = ShapelyPolygon(outer, [hole])

        pieces = split_at_dateline(poly)
        assert sum(p.area for p in pieces) == pytest.approx(poly.area)
        # The hole centre, once lon is still in range, must stay outside.
        hole_centre = ShapelyPoint(-170.0, 10.0)
        assert not any(p.contains(hole_centre) for p in pieces)


class TestMergePolygonsDateline:
    def test_mapbox_rectangle_via_merge_polygons(self):
        merged = merge_polygons(
            _multi_polygon(
                _rect(
                    -212.67488435641027,
                    -39.90820956224461,
                    -157.75735934215612,
                    -9.159204863042774,
                )
            )
        )
        assert len(merged) == 2
        for polygon in merged:
            min_lon, _, max_lon, _ = polygon.bounds
            assert -180.0 <= min_lon <= max_lon <= 180.0

    def test_whole_globe_stays_one_polygon(self):
        merged = merge_polygons(_multi_polygon(_rect(-180, -90, 180, 90)))
        assert len(merged) == 1
        assert merged[0].bounds == pytest.approx((-180, -90, 180, 90))

    def test_overlapping_unwrapped_rects_dissolve_then_split(self):
        # Two overlapping unwrapped boxes that both cross the dateline must
        # dissolve into one continuous region first, then split into two
        # pieces — not four shards.
        merged = merge_polygons(
            _multi_polygon(
                _rect(-210, 0, -160, 10),
                _rect(-200, 0, -150, 10),
            )
        )
        assert len(merged) == 2
        # Unwrapped union is lon -210..-150 (width 60) x height 10.
        assert sum(p.area for p in merged) == pytest.approx(600.0)

    def test_freeform_in_range_unchanged(self):
        ring = [[10, 0], [20, 0], [15, 10], [10, 0]]
        merged = merge_polygons(_multi_polygon(ring))
        assert len(merged) == 1
        assert merged[0].equals(ShapelyPolygon(ring))


class TestMultiPolygonHelper:
    def test_no_multi_polygon_means_no_bboxes(self):
        # All three views must agree that no spatial filter was given: [] flows
        # straight into subset_zarr, which slices by time only. Defaulting here
        # made .bboxes claim a filter existed while .polygons/.geometry did not.
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

    def test_mapbox_dateline_box_yields_two_bboxes_and_multipolygon(self):
        helper = MultiPolygonHelper(
            multi_polygon=_multi_polygon(
                _rect(
                    -212.67488435641027,
                    -39.90820956224461,
                    -157.75735934215612,
                    -9.159204863042774,
                )
            )
        )

        assert len(helper.bboxes) == 2
        assert helper.geometry.geom_type == "MultiPolygon"
        for bbox in helper.bboxes:
            assert -180.0 <= bbox.min_lon <= bbox.max_lon <= 180.0
            assert bbox.min_lat < bbox.max_lat
