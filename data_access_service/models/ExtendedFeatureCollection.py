from typing import Dict, Any, List

from geojson import FeatureCollection, Feature


class ExtendedFeatureCollection(FeatureCollection):
    """
    Extended FeatureCollection class with additional properties
    """

    def __init__(
        self, features: List[Feature], properties: Dict[str, Any], **extra: Any
    ):

        super().__init__(features, **extra)
        self.properties = properties
        self["type"] = "FeatureCollection"

    @property
    def properties(self) -> Dict[str, Any]:
        return self.get("properties", {})

    @properties.setter
    def properties(self, value: Dict[str, Any]) -> None:
        self["properties"] = value
