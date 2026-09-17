"""Types for the tiler's zarr -> parquet conversion.

The value data is sparse ``(timestamp, dataset, uuid, variable, i, j, value)``
rows, carrying pixel indices, not lat/lon degrees or CF metadata. Each store
gets one parquet file per variable. Everything a reader needs to turn that
back into the dense ``xr.Dataset`` the tiler rendering pipeline expects - the
lat/lon coordinate arrays, native grid shape, per-variable dtype/CF attrs
(``flag_values``/``flag_meanings``/``units``, read by the categorical and
point-query code paths), and the available timestamp list - lives in one JSON
sidecar per store (``metadata.json``), shared by every variable of that store
since they're all on the same grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Bumped when the sidecar's fields change in a way an older reader cannot
# handle. A reader that does not recognise the version falls back to the live
# zarr read rather than guessing at a possibly-incompatible layout.
TILER_PARQUET_METADATA_VERSION = 1


@dataclass(frozen=True)
class TilerVariableMetadata:
    """Per-variable facts the sparse value parquet does not carry."""

    dtype: str
    attrs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"dtype": self.dtype, "attrs": self.attrs}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TilerVariableMetadata":
        return cls(dtype=str(data["dtype"]), attrs=dict(data.get("attrs") or {}))


@dataclass(frozen=True)
class TilerParquetMetadata:
    """JSON sidecar written beside a store's value parquet(s) (``{uuid}.metadata.json``)."""

    version: int
    uuid: str
    dataset: str
    source_path: str
    n_i: int
    n_j: int
    lat: list[float]
    lon: list[float]
    timestamps: list[str]
    variables: dict[str, TilerVariableMetadata]
    schema_fingerprint: str
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "uuid": self.uuid,
            "dataset": self.dataset,
            "source_path": self.source_path,
            "n_i": self.n_i,
            "n_j": self.n_j,
            "lat": self.lat,
            "lon": self.lon,
            "timestamps": self.timestamps,
            "variables": {k: v.to_dict() for k, v in self.variables.items()},
            "schema_fingerprint": self.schema_fingerprint,
            "generated_at": self.generated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TilerParquetMetadata":
        return cls(
            version=int(data["version"]),
            uuid=str(data["uuid"]),
            dataset=str(data["dataset"]),
            source_path=str(data["source_path"]),
            n_i=int(data["n_i"]),
            n_j=int(data["n_j"]),
            lat=[float(x) for x in data["lat"]],
            lon=[float(x) for x in data["lon"]],
            timestamps=list(data["timestamps"]),
            variables={
                k: TilerVariableMetadata.from_dict(v)
                for k, v in data.get("variables", {}).items()
            },
            schema_fingerprint=str(data.get("schema_fingerprint", "")),
            generated_at=str(data.get("generated_at", "")),
        )


# Bumped when the manifest's fields change in a way an older reader cannot
# handle.
ROOT_METADATA_VERSION = 1


@dataclass(frozen=True)
class RootMetadata:
    """The batch-generated catalogue manifest (``root_metadata.json``, written
    once per batch run at the top of the output directory).

    Lists every product the batch successfully converted - full
    ``Product.to_dict()`` entries, so the tiler API can rebuild its product
    registry from this file alone, without calling live metadata or opening
    any zarr store itself.
    """

    version: int
    generated_at: str
    products: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "products": self.products,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootMetadata":
        return cls(
            version=int(data["version"]),
            generated_at=str(data.get("generated_at", "")),
            products=list(data.get("products", [])),
        )
