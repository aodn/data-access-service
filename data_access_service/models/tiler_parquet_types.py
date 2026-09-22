"""Types for the tiler's zarr -> parquet conversion.

Values are sparse ``(i, j, value)`` rows: pixel indices, no coordinates or CF
metadata. One parquet per variable per timestamp, so a batch run only adds
files and never rewrites them. Everything else the tiler needs - coordinates,
grid shape, per-variable dtype and attrs, converted timestamps - lives in one
``metadata.json`` per store, shared by all its variables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Output layout, shared by batch (writer) and tiler (reader). ``tiler_root_dir``
# is ``s3://{datavis_data bucket}/tiler`` (Config.get_tiler_root_dir):
#
#   {tiler_root_dir}/root_metadata.json
#   {tiler_root_dir}/{store}/metadata.json
#   {tiler_root_dir}/{store}/{variable}/{timestamp}.parquet
#
# ``store`` is the zarr dataset name minus ``.zarr``, ``variable`` is spelled
# as the zarr spells it, ``timestamp`` is the sidecar's string minus ":".


def _join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def root_metadata_path(tiler_root_dir: str) -> str:
    return _join(tiler_root_dir, "root_metadata.json")


def store_metadata_path(tiler_root_dir: str, store: str) -> str:
    return _join(tiler_root_dir, store, "metadata.json")


def variable_parquet_path(
    tiler_root_dir: str, store: str, variable: str, timestamp: str
) -> str:
    # "2024-01-15T13:00:00.000000000Z" -> "2024-01-15T130000.000000000Z"
    return _join(
        tiler_root_dir, store, variable, f"{timestamp.replace(':', '')}.parquet"
    )


@dataclass(frozen=True)
class TilerVariableMetadata:
    """Per-variable facts the sparse value parquet does not carry."""

    dtype: str
    attrs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"dtype": self.dtype, "attrs": self.attrs}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TilerVariableMetadata":
        return cls(
            dtype=str(data["dtype"]),
            attrs=dict(data.get("attrs") or {}),
        )


@dataclass(frozen=True)
class TilerParquetMetadata:
    """One store's sidecar (``metadata.json``). ``dataset`` is ``{store}.zarr``.

    ``timestamps`` lists only instants whose parquet is already written - batch
    saves it after the files. ``empty_timestamps`` are instants with no data at
    all: no files, never read again.
    """

    uuid: str
    dataset: str
    n_i: int
    n_j: int
    lat: list[float]
    lon: list[float]
    timestamps: list[str]
    variables: dict[str, TilerVariableMetadata]
    generated_at: str
    empty_timestamps: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "uuid": self.uuid,
            "dataset": self.dataset,
            "n_i": self.n_i,
            "n_j": self.n_j,
            "lat": self.lat,
            "lon": self.lon,
            "timestamps": self.timestamps,
            "empty_timestamps": self.empty_timestamps,
            "variables": {k: v.to_dict() for k, v in self.variables.items()},
            "generated_at": self.generated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TilerParquetMetadata":
        return cls(
            uuid=str(data["uuid"]),
            dataset=str(data["dataset"]),
            n_i=int(data["n_i"]),
            n_j=int(data["n_j"]),
            lat=[float(x) for x in data["lat"]],
            lon=[float(x) for x in data["lon"]],
            timestamps=list(data["timestamps"]),
            empty_timestamps=list(data.get("empty_timestamps", [])),
            variables={
                k: TilerVariableMetadata.from_dict(v)
                for k, v in data.get("variables", {}).items()
            },
            generated_at=str(data.get("generated_at", "")),
        )


@dataclass(frozen=True)
class ProductIdentity:
    """What a product is: store, variable(s), metadata record.

    Nothing about how it renders - ``visual``/``ocean_masked``/tile configs come
    from ``products_customisation`` and are applied by the tiler, so changing
    them needs no batch rerun.
    """

    id: str
    # Directory under tiler_root_dir: "foo" for foo.zarr.
    store: str
    variable: str | list[str]
    metadata_uuid: str | None = None

    @property
    def variables(self) -> list[str]:
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def to_dict(self) -> dict[str, Any]:
        """No ``store``: ``root_metadata.json`` keys its entries by it."""
        return {
            "id": self.id,
            "variable": self.variable,
            "metadata_uuid": self.metadata_uuid,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], store: str) -> "ProductIdentity":
        return cls(
            id=data["id"],
            store=store,
            variable=data["variable"],
            metadata_uuid=data.get("metadata_uuid"),
        )


# Bumped on an incompatible change; a file at another version is rewritten.
ROOT_METADATA_VERSION = 2


@dataclass(frozen=True)
class RootMetadata:
    """The catalogue manifest (``root_metadata.json``): every serveable store
    and its products, so the tiler can build its catalogue without live
    metadata or opening a zarr.

    A store batch failed to open keeps its old entry; one with no converted
    timestamps has none. Failures stay in the logs - the tiler never filters
    this file.
    """

    version: int
    generated_at: str
    stores: dict[str, list[ProductIdentity]]

    @property
    def products(self) -> list[ProductIdentity]:
        """Every store's products, flattened."""
        return [p for products in self.stores.values() for p in products]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "stores": {
                store: {"products": [p.to_dict() for p in products]}
                for store, products in self.stores.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RootMetadata":
        return cls(
            version=int(data["version"]),
            generated_at=str(data.get("generated_at", "")),
            stores={
                store: [
                    ProductIdentity.from_dict(p, store)
                    for p in (entry.get("products") or [])
                ]
                for store, entry in (data.get("stores") or {}).items()
            },
        )
