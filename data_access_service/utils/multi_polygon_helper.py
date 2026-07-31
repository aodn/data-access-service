"""Turning the user's drawn polygons into the shapes the subset paths use.

The portal sends a GeoJSON MultiPolygon whose parts may overlap. Every consumer
has to dissolve them the same way first - overlapping parts become one outline,
so the overlap is counted (queried, downloaded, estimated) once - and this
module is the single owner of that step, so the parquet and zarr paths cannot
drift apart.
"""

import geojson

from typing import List, Optional, Union
from geojson import MultiPolygon, Polygon
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import NON_SPECIFIED


def get_bbox_from(polygon: Polygon) -> BoundingBox:
    coordinates = list(geojson.utils.coords(polygon))
    lats = [coord[1] for coord in coordinates]
    lons = [coord[0] for coord in coordinates]

    return BoundingBox(
        min_lon=min(lons), min_lat=min(lats), max_lon=max(lons), max_lat=max(lats)
    )


def parse_multi_polygon(
    multi_polygon: Union[str, dict, MultiPolygon, None],
) -> Optional[MultiPolygon]:
    """Normalise the raw spatial filter into a geojson.MultiPolygon.

    Accepts a JSON string (batch job parameters), an already-parsed dict (the
    estimation route) or a geojson object. Returns None when the user asked for
    no spatial filter, so callers can tell "no area given" apart from "an area
    with no parts".

    :raises TypeError: the geometry parses but is not a MultiPolygon
    """
    if multi_polygon is None or multi_polygon == NON_SPECIFIED:
        return None

    if isinstance(multi_polygon, str):
        multi_polygon = geojson.loads(multi_polygon)
    elif isinstance(multi_polygon, dict) and not isinstance(
        multi_polygon, MultiPolygon
    ):
        multi_polygon = geojson.loads(geojson.dumps(multi_polygon))

    if not isinstance(multi_polygon, MultiPolygon):
        raise TypeError("Unsupported multi_polygon type")
    return multi_polygon


def merge_polygons(
    multi_polygon: Union[str, dict, MultiPolygon, None],
) -> List[ShapelyPolygon]:
    """Dissolve the drawn polygons into non-overlapping shapely polygons.

    Overlapping parts merge into one outline (their overlap is then counted
    once); disjoint parts stay separate. Holes drawn by the user are kept.

    Returns [] when there is no spatial filter (or the MultiPolygon has no
    parts) - the caller decides what that means.

    :raises ValueError: parts were given but none of them has any area
    """
    parsed = parse_multi_polygon(multi_polygon)
    if parsed is None:
        return []

    # part[0] is the exterior ring, part[1:] are the holes
    parts = [ShapelyPolygon(part[0], part[1:]) for part in parsed.coordinates]
    if not parts:
        return []

    merged = to_polygons(unary_union(parts))
    if not merged:
        # e.g. every part is a zero-area sliver. Returning [] here would be read
        # as "no spatial filter" and silently subset the whole globe.
        raise ValueError(f"multi_polygon encloses no area: {parsed}")
    return merged


def to_polygons(geometry: BaseGeometry) -> List[ShapelyPolygon]:
    """Flatten a unary_union result into a plain list of polygons with area.

    unary_union returns a Polygon, a MultiPolygon, or - when the parts include
    degenerate geometry - a GeometryCollection mixing lines/points in. Anything
    without area is dropped: it selects no cell to subset, and a zero-width
    bounding box is rejected by BoundingBox anyway.
    """
    if isinstance(geometry, ShapelyPolygon):
        return [geometry] if geometry.area > 0 else []
    if hasattr(geometry, "geoms"):
        return [
            geom
            for geom in geometry.geoms
            if isinstance(geom, ShapelyPolygon) and geom.area > 0
        ]
    return []


def bbox_of(polygon: ShapelyPolygon) -> BoundingBox:
    """The polygon's bounding box."""
    min_lon, min_lat, max_lon, max_lat = polygon.bounds
    return BoundingBox(
        min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat
    )


def as_one_geometry(polygons: List[ShapelyPolygon]) -> Optional[BaseGeometry]:
    """The polygons as ONE geometry, for point-in-polygon masking
    (subset_zarr_helper.form_geometry_mask). None for no polygons, which callers
    read as "no area given, do not mask".
    """
    if not polygons:
        return None
    if len(polygons) == 1:
        return polygons[0]
    return ShapelyMultiPolygon(polygons)


class MultiPolygonHelper:
    def __init__(self, multi_polygon: Union[str, dict, MultiPolygon, None]):

        # TODO: the zarr subsetting library can only slice by bbox, so the
        #  polygons are reduced to their bounding boxes here. The exact shapes
        #  are kept in .polygons so the batch path can mask the leftover cells.
        parsed_multipolygon = parse_multi_polygon(multi_polygon)

        # No multi_polygon means no spatial filter;
        if parsed_multipolygon is None:
            self._polygons: List[ShapelyPolygon] = []
            self._bboxes: List[BoundingBox] = []
            return

        self._polygons = merge_polygons(parsed_multipolygon)
        self._bboxes = [bbox_of(polygon) for polygon in self._polygons]

    @property
    def bboxes(self) -> List[BoundingBox]:
        """Get the read-only list of bounding boxes."""
        return self._bboxes

    @property
    def polygons(self) -> List[ShapelyPolygon]:
        """The merged polygons the bboxes came from, in the same order."""
        return self._polygons

    @property
    def geometry(self) -> Optional[BaseGeometry]:
        """The merged polygons as one geometry; None when no area was given."""
        return as_one_geometry(self._polygons)
