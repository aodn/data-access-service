import json

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.tasks.generate_dataset import get_lat_lon_from_


class MultiPolygonHelper:
    def __init__(self, multi_polygon: str):

        # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
        #  we can change to use the polygon coordinates directly

        self.bboxes = []
        multi_polygon = json.loads(multi_polygon)
        for polygon in multi_polygon["coordinates"]:
            lats_lons = get_lat_lon_from_(polygon=polygon)
            min_lat = lats_lons["min_lat"]
            max_lat = lats_lons["max_lat"]
            min_lon = lats_lons["min_lon"]
            max_lon = lats_lons["max_lon"]
            bbox = BoundingBox(
                min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat
            )
            self.bboxes.append(bbox)
