import decimal
from dataclasses import dataclass


@dataclass
class Depth:
    min: decimal
    max: decimal
    unit: str

@dataclass
class Coordinate:
    valid_min: decimal
    valid_max: decimal

@dataclass
class Descriptor:
    uuid: str
    dname: str
    latitude: Coordinate = None
    longitude: Coordinate = None
    depth: Depth = None
