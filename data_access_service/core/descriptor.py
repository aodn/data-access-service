from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
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


class KeyType(str, Enum):
    """The kind of data behind a key."""

    CLOUD_OPTIMISED = "cloud_optimised"
    DATA_INDEX = "data_index"


@dataclass
class Descriptor:
    uuid: Final[str]
    dname: Final[str]
    lat: Final[Coordinate]
    lng: Final[Coordinate]
    depth: Final[Depth]
    type: Final[KeyType]

    def __init__(
        self,
        uuid: str,
        dname: str = None,
        lat: Coordinate = None,
        lng: Coordinate = None,
        depth: Depth = None,
        type: KeyType = KeyType.CLOUD_OPTIMISED,
    ):
        self.uuid = uuid
        self.dname = dname
        self.depth = depth
        self.lat = lat
        self.lng = lng
        self.type = type
