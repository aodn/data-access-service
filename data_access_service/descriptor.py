import decimal
from dataclasses import dataclass


@dataclass
class Depth:
    min: decimal
    max: decimal
    unit: str


@dataclass
class Descriptor:
    uuid: str
    key: str
    depth: Depth = None

    def __init__(self, uuid: str, key: str = None, depth: Depth = None):
        self.uuid = uuid
        self.key = key
        self.depth = depth
