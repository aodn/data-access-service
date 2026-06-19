"""Export a subset Zarr Dataset to GeoTIFF files in a ZIP archive."""

import gc
import zipfile
from pathlib import Path

import numpy as np
import xarray

# Treat differences below this (degrees) as zero, to absorb float rounding.
REGULAR_GRID_TOLERANCE = 1e-6


def write_all_tifs_to_zip(
    zip_path: Path,
    dataset: xarray.Dataset,
    dataset_base: str,
    data_vars: list,
    time_name: str,
    lat_name: str,
    lon_name: str,
    lat_ascending: bool,
    is_curvilinear: bool,
    work_dir: Path,
    log=None,
) -> None:
    """Write one TIF per variable per time step into a single ZIP archive."""
    time_values = dataset[time_name].values

    # A curvilinear grid stores lat/lon as 2D arrays that don't change over time.
    lat_2d = dataset[lat_name].values if is_curvilinear else None
    lon_2d = dataset[lon_name].values if is_curvilinear else None

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
                # handled explicitly in regular_slice_to_geotiff.
                slice_data = dataset[var_name].sel({time_name: t}).compute()

                tif_name = f"{dataset_base}_{var_name}_{date_str}.tif"
                tif_path = work_dir / tif_name

                if is_curvilinear:
                    curvilinear_slice_to_geotiff(
                        slice_data, tif_path, lat_2d, lon_2d, log
                    )
                else:
                    regular_slice_to_geotiff(
                        slice_data, tif_path, lat_name, lon_name, lat_ascending, log
                    )

                zf.write(tif_path, arcname=f"{var_name}/{tif_name}")
                tif_path.unlink(missing_ok=True)

                del slice_data
                gc.collect()


def regular_slice_to_geotiff(
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


def curvilinear_slice_to_geotiff(
    slice_data: xarray.DataArray,
    tif_path: Path,
    lat_2d: np.ndarray,
    lon_2d: np.ndarray,
    log=None,
) -> None:
    """Warp a curvilinear (I, J) slice onto a regular lat/lon GeoTIFF.

    A curvilinear grid has no straight axes, so we anchor a sample of pixels to
    their true lon/lat (ground control points) and let GDAL warp the rest - the
    standard way to handle satellite swath / rotated grids.
    """
    import rioxarray  # noqa: F401
    from rasterio.control import GroundControlPoint
    from rasterio.enums import Resampling

    # 1. Reduce to 2D: drop extra dims like depth (take the first index).
    extra_dims = [d for d in slice_data.dims if d not in ("I", "J")]
    for dim in extra_dims:
        slice_data = slice_data.isel({dim: 0})
    if extra_dims and log:
        log.warning(f"Selected first index for extra dims: {extra_dims}")

    # 2. Use float so missing cells can be marked with NaN nodata.
    if slice_data.dtype.kind in ("i", "u"):
        slice_data = slice_data.astype(np.float32)

    # 3. Build ground control points: sample ~20 pixels per axis and record each
    #    one's true lon/lat. Example: with 100 rows, take every 5th plus the last.
    n_i, n_j = lat_2d.shape
    rows = sorted(set(range(0, n_i, max(1, n_i // 20))) | {n_i - 1})
    cols = sorted(set(range(0, n_j, max(1, n_j // 20))) | {n_j - 1})
    gcps = [
        GroundControlPoint(row=i, col=j, x=float(lon_2d[i, j]), y=float(lat_2d[i, j]))
        for i in rows
        for j in cols
        if np.isfinite(lat_2d[i, j]) and np.isfinite(lon_2d[i, j])
    ]

    # 4. Attach the GCPs and warp onto a regular WGS84 grid (bilinear resampling).
    slice_data = slice_data.rio.set_spatial_dims(x_dim="J", y_dim="I")
    slice_data = slice_data.rio.write_gcps(gcps, "EPSG:4326")
    slice_data = slice_data.rio.write_nodata(np.nan)
    warped = slice_data.rio.reproject("EPSG:4326", resampling=Resampling.bilinear)

    warped.rio.to_raster(str(tif_path))


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


def prepare_grid_for_geotiff(
    dataset: xarray.Dataset, lat_name: str, lon_name: str, log=None
) -> xarray.Dataset:
    """Get a 2D (I, J) grid ready for GeoTIFF.

    A regular grid is reduced to 1D lat/lon here; a curvilinear grid is left as
    2D and gets warped per slice at write time.
    """
    lat_drift, lon_drift = grid_drift(
        dataset[lat_name].values, dataset[lon_name].values
    )
    if is_regular_grid(lat_drift, lon_drift):
        if log:
            log.info("Regular grid, reducing I/J to 1D lat/lon")
        return convert_ij_dims_to_latlon(dataset, lat_name, lon_name, log)

    if log:
        log.info(
            f"Curvilinear grid (lat drift {lat_drift:.4f}, lon drift "
            f"{lon_drift:.4f} deg), will warp to a regular grid for GeoTIFF"
        )
    return dataset


def has_ij_dims(dataset: xarray.Dataset) -> bool:
    """True if the dataset still has raw I/J axes (a 2D, not-yet-reduced grid)."""
    return "I" in dataset.dims and "J" in dataset.dims


def grid_drift(lat_2d: np.ndarray, lon_2d: np.ndarray):
    """How much the grid bends: max lat change along a row, lon change down a column."""
    # Example: row [-30, -30.02, -30.04] varies by up to 0.04, so its drift is 0.04.
    lat_drift = float(np.nanmax(np.abs(lat_2d - lat_2d[:, [0]])))
    lon_drift = float(np.nanmax(np.abs(lon_2d - lon_2d[[0], :])))
    return lat_drift, lon_drift


def is_regular_grid(
    lat_drift: float, lon_drift: float, tolerance: float = REGULAR_GRID_TOLERANCE
) -> bool:
    """True if the grid is effectively straight (it bends less than the tolerance)."""
    return lat_drift <= tolerance and lon_drift <= tolerance


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
