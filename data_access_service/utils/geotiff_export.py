"""Export a subset Zarr Dataset to GeoTIFF files in a ZIP archive."""

import gc
import zipfile
from pathlib import Path

import numpy as np
import xarray


def convert_ij_dims_to_latlon(
    dataset: xarray.Dataset, lat_name: str, lon_name: str, log=None
) -> xarray.Dataset:
    """Convert (TIME, I, J) datasets to (TIME, LATITUDE, LONGITUDE)."""
    lats = dataset[lat_name].values[:, 0]
    lons = dataset[lon_name].values[0, :]

    if log:
        log.info(
            f"Converting I({len(lats)})→{lat_name}, " f"J({len(lons)})→{lon_name}"
        )

    # Replace 2D index dims (I,J) with 1D coordinate dims (lat,lon)
    dataset = dataset.drop_vars([lat_name, lon_name])
    dataset = dataset.rename({"I": lat_name, "J": lon_name})
    dataset = dataset.assign_coords({lat_name: lats, lon_name: lons})

    return dataset


def get_geotiff_compatible_vars(
    dataset: xarray.Dataset, lat_name: str, lon_name: str
) -> list:
    """Filter variables that can be exported as GeoTIFF: must be numeric and have lat/lon dims."""
    results = []
    for var in dataset.data_vars:
        is_numeric = dataset[var].dtype.kind in ("i", "u", "f")
        has_latlon = lat_name in dataset[var].dims and lon_name in dataset[var].dims
        if is_numeric and has_latlon:
            results.append(var)
    return results


def is_lat_ascending(dataset: xarray.Dataset, lat_name: str, log=None) -> bool:
    """Check if latitude is ascending (south-to-north). Rasterio expects descending."""
    lat_values = dataset[lat_name].values
    ascending = lat_values[0] < lat_values[-1] if len(lat_values) > 1 else False
    if ascending and log:
        log.info(
            f"Latitude is ascending ({lat_values[0]} -> {lat_values[-1]}), "
            "will sort descending for rasterio"
        )
    return ascending


def write_all_tifs_to_zip(
    zip_path: Path,
    dataset: xarray.Dataset,
    dataset_base: str,
    data_vars: list,
    time_name: str,
    lat_name: str,
    lon_name: str,
    lat_ascending: bool,
    work_dir: Path,
    log=None,
) -> None:
    """Write one TIF per variable per time step into a single ZIP archive."""
    time_values = dataset[time_name].values

    if log:
        log.info(
            f"GeoTIFF: {len(time_values)} time step(s), "
            f"{len(data_vars)} variable(s) -> {zip_path.name}"
        )

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for t in sorted(time_values):
            date_str = str(np.datetime_as_string(t, unit="D"))

            for var_name in data_vars:
                # Don't .squeeze() here: a bbox that selects a single
                # lat or lon cell would collapse that dim away, breaking
                # set_spatial_dims later. Extra non-spatial dims are
                # handled explicitly in slice_to_geotiff.
                slice_data = dataset[var_name].sel({time_name: t}).compute()

                tif_name = f"{dataset_base}_{var_name}_{date_str}.tif"
                tif_path = work_dir / tif_name

                slice_to_geotiff(
                    slice_data, tif_path, lat_name, lon_name, lat_ascending, log
                )

                zf.write(tif_path, arcname=f"{var_name}/{tif_name}")
                tif_path.unlink(missing_ok=True)

                del slice_data
                gc.collect()


def slice_to_geotiff(
    slice_data: xarray.DataArray,
    tif_path: Path,
    lat_name: str,
    lon_name: str,
    lat_ascending: bool,
    log=None,
) -> None:
    """Write a single 2D DataArray slice to a GeoTIFF file."""
    if lat_ascending:
        slice_data = slice_data.sortby(lat_name, ascending=False)

    # Ensure the slice is exactly 2D (lat, lon). Extra dims left after
    # squeeze() (e.g. depth, level) would produce a multi-band TIF that
    # QGIS may not open correctly.
    if slice_data.ndim != 2:
        extra_dims = [d for d in slice_data.dims if d not in (lat_name, lon_name)]
        for dim in extra_dims:
            slice_data = slice_data.isel({dim: 0})
        if log:
            log.warning(
                f"Slice had {slice_data.ndim + len(extra_dims)} dims, "
                f"selected first index for extra dims: {extra_dims}"
            )

    # Convert integer data to float so NaN nodata is representable
    if slice_data.dtype.kind in ("i", "u"):
        slice_data = slice_data.astype(np.float32)

    slice_data.attrs.pop("_FillValue", None)
    slice_data.encoding.pop("_FillValue", None)

    slice_data = slice_data.rio.set_spatial_dims(x_dim=lon_name, y_dim=lat_name)
    slice_data = slice_data.rio.write_crs("EPSG:4326")
    slice_data = slice_data.rio.write_nodata(np.nan)

    slice_data.rio.to_raster(str(tif_path))
