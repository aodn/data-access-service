from typing import List, Dict, Any


class Feature:
    def __init__(self, geometry: Dict[str, Any], properties: Dict[str, Any]):
        self.type = "Feature"
        self.geometry = geometry
        self.properties = properties


    def to_dict(self) -> Dict[str, Any]:
        feature = {
            "type": self.type,
            "geometry": self.geometry,
            "properties": self.properties,
        }
        return feature


class FeatureCollection:
    def __init__(self, features: List[Feature], properties: Dict[str, Any] = None):
        self.type = "FeatureCollection"
        self.features = features
        self.properties = properties

    def to_dict(self) -> Dict[str, Any]:
        collection = {
            "type": self.type,
            "features": [feature.to_dict() for feature in self.features],
        }
        if self.properties:
            collection["properties"] = self.properties
        return collection

    def add_feature(self, feature: Feature):
        """
        Add a feature to the collection.
        """
        if not isinstance(feature, Feature):
            raise TypeError("feature must be an instance of Feature")
        self.features.append(feature)