import geojson

from typing import List, Union
from data_access_service.models.bounding_box import BoundingBox
from geojson import MultiPolygon, Polygon


def get_bbox_from(polygon: Polygon) -> BoundingBox:
    coordinates = list(geojson.utils.coords(polygon))
    lats = [coord[1] for coord in coordinates]
    lons = [coord[0] for coord in coordinates]

    return BoundingBox(
        min_lon=min(lons), min_lat=min(lats), max_lon=max(lons), max_lat=max(lats)
    )


class MultiPolygonHelper:
    def __init__(self, multi_polygon: Union[str, MultiPolygon, None]):

        # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
        #  we can change to use the polygon coordinates directly

        # if users do not specify multi_polygon, assume whole globe
        if multi_polygon is None or multi_polygon == "non-specified":
            # whole globe
            self._bboxes = [
                BoundingBox(min_lon=-180, min_lat=-90, max_lon=180, max_lat=90)
            ]
            return

        self._bboxes: List[BoundingBox] = []

        if isinstance(multi_polygon, str):
            multi_polygon = geojson.loads(multi_polygon)

        if isinstance(multi_polygon, MultiPolygon):
            coordinates = multi_polygon.coordinates
        else:
            raise TypeError("Unsupported multi_polygon type")

        for polygon in coordinates:
            self._bboxes.append(get_bbox_from(polygon=polygon))

    @property
    def bboxes(self) -> List[BoundingBox]:
        """Get the read-only list of bounding boxes."""
        return self._bboxes
