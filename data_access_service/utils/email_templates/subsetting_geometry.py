"""Classify a subset request's geometry for email rendering.

Splits a GeoJSON MultiPolygon into bounding boxes and freeform polygons so the
download email can show each with the right layout. Mirrors the ogcapi-java
EmailUtils split.
"""

import geojson

from typing import List, Tuple, Union
from geojson import MultiPolygon

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.multi_polygon_helper import get_bbox_from

# A rectangle bounding box has 4 corners; a ring with more unique vertices
# than this is treated as a freeform polygon rather than a bounding box.
_MAX_BBOX_VERTICES = 4


def _remove_closing_point(ring: list) -> list:
    """Return a ring's unique vertices, dropping the GeoJSON closing point
    (the last vertex, which repeats the first)."""
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
    """Split a GeoJSON MultiPolygon into bounding boxes and freeform polygons.

    Each ring is classified by its number of unique vertices: 4 or fewer is a
    bounding box, more than 4 is a freeform polygon (kept as its [lon, lat]
    vertices). Whole-globe bounding boxes are dropped since they mean "no area
    filter".
    """
    bboxes: List[BoundingBox] = []
    polygons: List[list] = []

    # No area selected -> nothing spatial to render.
    if multi_polygon is None or multi_polygon == "non-specified":
        return bboxes, polygons

    if isinstance(multi_polygon, str):
        multi_polygon = geojson.loads(multi_polygon)
    if not isinstance(multi_polygon, MultiPolygon):
        raise TypeError("Unsupported multi_polygon type")

    for polygon in multi_polygon.coordinates:
        outer_ring = polygon[0] if polygon else []
        vertices = _remove_closing_point(outer_ring)

        # More corners than a rectangle -> render it as a freeform polygon.
        if len(vertices) > _MAX_BBOX_VERTICES:
            polygons.append(vertices)
            continue

        # Otherwise reduce the rectangle to a bounding box, skipping the
        # whole-globe default which represents no spatial filter.
        bbox = get_bbox_from(polygon=polygon)
        if not _is_global_extent(bbox):
            bboxes.append(bbox)

    return bboxes, polygons
