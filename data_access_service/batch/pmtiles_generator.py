# Compatibility shim — re-export public functions/types from the new pmtiles package
from data_access_service.batch.pmtiles import (
    HexLayerSpec,
    generate_pmtiles,
    generate_pmtiles_for,
    generate_pmtiles_for_all_parquets,
)

__all__ = [
    "HexLayerSpec",
    "generate_pmtiles",
    "generate_pmtiles_for",
    "generate_pmtiles_for_all_parquets",
]
