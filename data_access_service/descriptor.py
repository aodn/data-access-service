import decimal
from dataclasses import dataclass


@dataclass
class Depth:
    min: decimal
    max: decimal
    unit: str


@dataclass(init=True)
class Descriptor:
    uuid: str
    depth: Depth = None

    def __init__(self, uuid: str, depth: Depth = None):
        self.uuid = uuid
        self.depth = depth