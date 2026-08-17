"""Split a subset request's GeoJSON MultiPolygon into bounding boxes and
freeform polygons so the email can render each with the right layout."""

import geojson

from typing import List, Tuple, Union
from geojson import MultiPolygon

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import NON_SPECIFIED
from data_access_service.utils.multi_polygon_helper import get_bbox_from


def _is_axis_aligned_rectangle(vertices: list) -> bool:
    """True when the ring is an axis-aligned rectangle (2 lons x 2 lats)."""
    unique_corners = {(vertex[0], vertex[1]) for vertex in vertices}
    unique_lons = {lon for lon, _ in unique_corners}
    unique_lats = {lat for _, lat in unique_corners}

    # 4 distinct corners drawn from only 2 longitudes x 2 latitudes can only
    # be the min/max corner combinations of a rectangle.
    return len(unique_corners) == 4 and len(unique_lons) == 2 and len(unique_lats) == 2


def _remove_closing_point(ring: list) -> list:
    """Drop the GeoJSON closing point (the last vertex repeats the first)."""
    vertices = list(ring)
    if len(vertices) > 1 and vertices[0] == vertices[-1]:
        vertices = vertices[:-1]
    return vertices


def _is_global_extent(bbox: BoundingBox) -> bool:
    """Whole-globe bounding box, i.e. the default used when no area is selected."""
    return (
        bbox.min_lon == -180
        and bbox.min_lat == -90
        and bbox.max_lon == 180
        and bbox.max_lat == 90
    )


def split_bboxes_and_polygons(
    multi_polygon: Union[str, MultiPolygon, None],
) -> Tuple[List[BoundingBox], List[list]]:
    """Split each ring by shape: axis-aligned rectangles become bounding boxes,
    any other shape stays a polygon; whole-globe boxes (no area filter) are dropped."""
    bboxes: List[BoundingBox] = []
    polygons: List[list] = []

    # No area selected -> nothing spatial to render.
    if multi_polygon is None or multi_polygon == NON_SPECIFIED:
        return bboxes, polygons

    if isinstance(multi_polygon, str):
        multi_polygon = geojson.loads(multi_polygon)
    if not isinstance(multi_polygon, MultiPolygon):
        raise TypeError("Unsupported multi_polygon type")

    for polygon in multi_polygon.coordinates:
        outer_ring = polygon[0] if polygon else []
        vertices = _remove_closing_point(outer_ring)

        # Anything that is not an axis-aligned rectangle keeps its vertices.
        if not _is_axis_aligned_rectangle(vertices):
            polygons.append(vertices)
            continue

        # Otherwise reduce the rectangle to a bounding box, skipping the
        # whole-globe default which represents no spatial filter.
        bbox = get_bbox_from(polygon=polygon)
        if not _is_global_extent(bbox):
            bboxes.append(bbox)

    return bboxes, polygons
