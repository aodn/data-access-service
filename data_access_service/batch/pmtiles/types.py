from dataclasses import dataclass


@dataclass(frozen=True)
class HexLayerSpec:
    name: str
    h3_resolution: int
    minzoom: int
    maxzoom: int
    output_path: str
