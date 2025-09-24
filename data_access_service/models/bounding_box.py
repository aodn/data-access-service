class BoundingBox:
    def __init__(self, min_lon: float, min_lat: float, max_lon: float, max_lat: float):
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError("Invalid bounding box coordinates")
        self.min_lon = min_lon
        self.min_lat = min_lat
        self.max_lon = max_lon
        self.max_lat = max_lat
