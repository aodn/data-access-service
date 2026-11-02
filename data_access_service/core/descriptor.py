from decimal import Decimal
from dataclasses import dataclass
from typing import Final


@dataclass
class Depth:
    min: Final[Decimal]
    max: Final[Decimal]
    unit: Final[str]


@dataclass
class Coordinate:
    min: Final[Decimal]
    max: Final[Decimal]


@dataclass
class Descriptor:
    uuid: Final[str]
    dname: Final[str]
    lat: Final[Coordinate]
    lng: Final[Coordinate]
    depth: Final[Depth]
    cloud_optimised: Final[bool]

    def __init__(
        self,
        uuid: str,
        dname: str = None,
        lat: Coordinate = None,
        lng: Coordinate = None,
        depth: Depth = None,
        cloud_optimised: bool = True,
    ):
        self.uuid = uuid
        self.dname = dname
        self.depth = depth
        self.lat = lat
        self.lng = lng
        self.cloud_optimised = cloud_optimised
