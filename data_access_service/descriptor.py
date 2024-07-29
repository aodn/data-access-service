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
    dname: str
    depth: Depth = None

    def __init__(self, uuid: str, dname: str = None, depth: Depth = None):
        self.uuid = uuid
        self.dname = dname
        self.depth = depth
