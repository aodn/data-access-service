"""Types for the tiler's zarr -> parquet conversion.

The value data is sparse ``(i, j, value)`` rows, carrying pixel indices, not
lat/lon degrees or CF metadata. Each store gets one parquet file per variable
per timestamp, so a batch run only ever adds files for new timestamps and
never rewrites old ones. Everything a reader needs to turn that back into the
dense ``xr.Dataset`` the tiler rendering pipeline expects - the lat/lon
coordinate arrays, native grid shape, per-variable dtype/CF attrs
(``flag_values``/``flag_meanings``/``units``, read by the categorical and
point-query code paths), and the list of converted timestamps - lives in one
JSON sidecar per store (``metadata.json``), shared by every variable of that
store since they're all on the same grid.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Output layout, shared by batch (writer) and tiler (reader). ``output_dir``
# is ``s3://{datavis_data bucket}/tiler`` (Config.get_tiler_output_dir):
#
#   {output_dir}/root_metadata.json
#   {output_dir}/{store}/metadata.json
#   {output_dir}/{store}/{variable}/{timestamp}.parquet
#
# ``store`` is ``ProductIdentity.store``: the zarr dataset name minus ``.zarr``.
# ``timestamp`` is the sidecar's own timestamp string with ":" dropped.


def _join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *parts])


def root_metadata_path(output_dir: str) -> str:
    return _join(output_dir, "root_metadata.json")


def store_metadata_path(output_dir: str, store: str) -> str:
    return _join(output_dir, store, "metadata.json")


def variable_parquet_path(
    output_dir: str, store: str, variable: str, timestamp: str
) -> str:
    # "2024-01-15T13:00:00.000000000Z" -> "2024-01-15T130000.000000000Z"
    return _join(output_dir, store, variable, f"{timestamp.replace(':', '')}.parquet")


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
    """JSON sidecar written beside a store's value parquet(s) (``metadata.json``).

    ``dataset`` is the source zarr dataset name (``{store}.zarr``).
    ``timestamps`` lists only the instants whose parquet files are written -
    batch updates it after the files, so a reader never sees one without them.
    ``empty_timestamps`` lists instants batch read but found no data in; they
    have no files and are never read again.
    """

    uuid: str
    dataset: str
    n_i: int
    n_j: int
    lat: list[float]
    lon: list[float]
    timestamps: list[str]
    variables: dict[str, TilerVariableMetadata]
    schema_fingerprint: str
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
            "schema_fingerprint": self.schema_fingerprint,
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
            schema_fingerprint=str(data.get("schema_fingerprint", "")),
            generated_at=str(data.get("generated_at", "")),
        )


@dataclass(frozen=True)
class ProductIdentity:
    """A batch-discovered product's identity: which store, which variable(s),
    which metadata collection. Nothing about how the tiler renders it -
    ``visual``/``ocean_masked``/tile configs are ``products_customisation``
    config, resolved only on the live tiler side (see
    ``tiler.services.product.catalog``) so a config-only change never
    requires a batch rerun.
    """

    id: str
    # Store name under output_dir (see the layout above), e.g. "foo" for foo.zarr.
    store: str
    variable: str | list[str]
    metadata_uuid: str | None = None

    @property
    def variables(self) -> list[str]:
        return self.variable if isinstance(self.variable, list) else [self.variable]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "store": self.store,
            "variable": self.variable,
            "metadata_uuid": self.metadata_uuid,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProductIdentity":
        return cls(
            id=data["id"],
            store=data["store"],
            variable=data["variable"],
            metadata_uuid=data.get("metadata_uuid"),
        )


# Bumped when the manifest's fields change in a way an older reader cannot
# handle.
ROOT_METADATA_VERSION = 1


@dataclass(frozen=True)
class RootMetadata:
    """The batch-generated catalogue manifest (``root_metadata.json``, written
    once per batch run at the top of the output directory).

    Lists every product the batch successfully converted - ``ProductIdentity``
    entries, so the tiler API can rebuild its product catalogue from this file
    alone (layering ``products_customisation`` on top itself), without calling
    live metadata or opening any zarr store.
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
