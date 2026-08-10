"""Output format rules, shared by the size estimate and the download.

The format implies the storage type it comes from: csv from a parquet key,
netcdf and geotiff from a zarr key. Any other pairing is a malformed request.

`api` stays a parameter, never an import, so utils does not depend on core.API.
"""

from typing import Optional

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource

from data_access_service.utils.geotiff_export import geotiff_eligible_vars

OUTPUT_FORMAT_CSV = "csv"
OUTPUT_FORMAT_NETCDF = "netcdf"
OUTPUT_FORMAT_GEOTIFF = "geotiff"
SUPPORTED_OUTPUT_FORMATS = frozenset(
    {OUTPUT_FORMAT_NETCDF, OUTPUT_FORMAT_GEOTIFF, OUTPUT_FORMAT_CSV}
)

KEY_SUFFIX_PARQUET = ".parquet"
KEY_SUFFIX_ZARR = ".zarr"
KEY_SUFFIX_FOR_FORMAT = {
    OUTPUT_FORMAT_CSV: KEY_SUFFIX_PARQUET,
    OUTPUT_FORMAT_NETCDF: KEY_SUFFIX_ZARR,
    OUTPUT_FORMAT_GEOTIFF: KEY_SUFFIX_ZARR,
}
KNOWN_KEY_SUFFIXES = (KEY_SUFFIX_PARQUET, KEY_SUFFIX_ZARR)


def check_storage_supports_format(
    key: str, key_suffix: Optional[str], output_format: str
) -> None:
    """Raise when storage type `key_suffix` cannot produce `output_format`.

    The single owner of the rule. A None `key_suffix` means the storage type is
    unknown, so nothing is checked; `key` is only used in the message.
    """
    if key_suffix is None:
        return
    expected_suffix = KEY_SUFFIX_FOR_FORMAT[output_format]
    if key_suffix != expected_suffix:
        raise ValueError(
            f"'{output_format}' export not possible for {key}: it downloads "
            f"from {expected_suffix} keys only."
        )


def check_key_supports_format(key: str, output_format: str) -> None:
    """Raise when `key`'s storage type cannot produce `output_format`.

    For callers holding only the key name. A key with neither suffix (the
    "not_exist" sentinel for an unknown uuid) is left alone.
    """
    suffix = next((s for s in KNOWN_KEY_SUFFIXES if key.endswith(s)), None)
    check_storage_supports_format(key, suffix, output_format)


def check_datasource_supports_format(
    api, ds, uuid: str, key: str, output_format: str
) -> None:
    """Raise when `ds` cannot produce `output_format`.

    For callers holding the resolved datasource, which beats the key name - a
    key without a known suffix still has a real storage type. Geotiff also
    needs a gridded variable, which only the store can answer.
    """
    if isinstance(ds, ZarrDataSource):
        key_suffix = KEY_SUFFIX_ZARR
    elif isinstance(ds, ParquetDataSource):
        key_suffix = KEY_SUFFIX_PARQUET
    else:
        key_suffix = None
    check_storage_supports_format(key, key_suffix, output_format)

    if isinstance(ds, ZarrDataSource) and output_format == OUTPUT_FORMAT_GEOTIFF:
        lat_name, lon_name, _ = api.resolve_dim_names(uuid, key)
        # Dims and dtypes never change under slicing, so the raw store answers
        # this without any subsetting work.
        if not geotiff_eligible_vars(ds.zarr_store, lat_name, lon_name):
            raise ValueError(
                f"GeoTIFF export not possible for {key}: no gridded numeric "
                "variables (a variable must be on both the lat and lon axes)."
            )
