"""Masks and coastal fill.

- ``inpaint_nearest``: fill gaps near the coast from the nearest value.
- ``land_mask_for_*``: a land mask, to cut filled values off land.
- ``apply_ocean_mask``: null values outside the sea-level model's valid ocean,
  on the raw slice. Only for products on that model's grid.
"""

from pathlib import Path

import numpy as np
import xarray as xr
from scipy.ndimage import distance_transform_edt

from data_access_service.config.tiler.paths import LAND_MASK_PATH, OCEAN_MASK_PATH

# Loaded on first use.
_land_mask: np.ndarray | None = None
_land_meta: dict[str, float] | None = None
_ocean_mask: np.ndarray | None = None
_ocean_meta: dict[str, float] | None = None


def load_land_mask() -> tuple[np.ndarray, dict[str, float]]:
    """The global land grid (True = land, north to south) and its metadata."""
    global _land_mask, _land_meta
    if _land_mask is None:
        path = Path(LAND_MASK_PATH)
        if not path.exists():
            raise FileNotFoundError(
                f"Land-mask asset not found at {path}. Generate it with "
                "`uv run --with regionmask --with cartopy --with pooch "
                "python scripts/build_land_mask.py`."
            )
        with np.load(path) as npz:
            shape = tuple(int(x) for x in npz["shape"])
            n = shape[0] * shape[1]
            _land_mask = np.unpackbits(npz["packed"])[:n].astype(bool).reshape(shape)
            _land_meta = {
                "res": float(npz["res"]),
                "lon_min": float(npz["lon_min"]),
                "lat_max": float(npz["lat_max"]),
            }
    assert _land_meta is not None
    return _land_mask, _land_meta


def land_mask_for_coords(lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """A (len(lats), len(lons)) land mask for these coords, nearest point."""
    land, meta = load_land_mask()
    h_src, w_src = land.shape
    res = meta["res"]

    lons = ((np.asarray(lons, dtype=float) + 180.0) % 360.0) - 180.0  # to [-180, 180)
    lats = np.asarray(lats, dtype=float)

    cols = np.floor((lons - meta["lon_min"]) / res).astype(np.intp)
    rows = np.floor((meta["lat_max"] - lats) / res).astype(np.intp)
    np.clip(cols, 0, w_src - 1, out=cols)
    np.clip(rows, 0, h_src - 1, out=rows)

    return land[np.ix_(rows, cols)]


def land_mask_for_grid(
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    total_w: int,
    total_h: int,
) -> np.ndarray:
    """A land mask on the (total_h, total_w) render grid."""
    lons = np.linspace(lon_min, lon_max, total_w)
    lats = np.linspace(lat_max, lat_min, total_h)  # north to south
    return land_mask_for_coords(lons, lats)


def load_ocean_mask() -> tuple[np.ndarray, dict[str, float]]:
    """The sea-level model's valid-ocean grid (north to south) and its
    metadata."""
    global _ocean_mask, _ocean_meta
    if _ocean_mask is None:
        path = Path(OCEAN_MASK_PATH)
        if not path.exists():
            raise FileNotFoundError(
                f"Ocean-mask asset not found at {path}. Generate it with "
                "`uv run --with h5netcdf --with h5py python scripts/build_ocean_mask.py`."
            )
        with np.load(path) as npz:
            shape = tuple(int(x) for x in npz["shape"])
            n = shape[0] * shape[1]
            _ocean_mask = np.unpackbits(npz["packed"])[:n].astype(bool).reshape(shape)
            _ocean_meta = {
                "res": float(npz["res"]),
                "lon_min": float(npz["lon_min"]),
                "lat_max": float(npz["lat_max"]),
            }
    assert _ocean_meta is not None
    return _ocean_mask, _ocean_meta


def ocean_valid_for_coords(lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """A (len(lats), len(lons)) valid-ocean mask; outside the mask is invalid."""
    mask, meta = load_ocean_mask()
    h_src, w_src = mask.shape
    res = meta["res"]
    lons = np.asarray(lons, dtype=float)
    lats = np.asarray(lats, dtype=float)

    cols = np.round((lons - meta["lon_min"]) / res).astype(np.intp)
    rows = np.round((meta["lat_max"] - lats) / res).astype(np.intp)
    in_bounds_c = (cols >= 0) & (cols < w_src)
    in_bounds_r = (rows >= 0) & (rows < h_src)
    np.clip(cols, 0, w_src - 1, out=cols)
    np.clip(rows, 0, h_src - 1, out=rows)

    valid = mask[np.ix_(rows, cols)]
    valid &= in_bounds_r[:, None] & in_bounds_c[None, :]
    return valid


def apply_ocean_mask(ds: xr.Dataset, variables: list[str]) -> xr.Dataset:
    """``ds`` with ``variables`` set to NaN outside the valid ocean."""
    valid = ocean_valid_for_coords(ds.lon.values, ds.lat.values)  # (lat, lon)
    valid_da = xr.DataArray(
        valid, dims=("lat", "lon"), coords={"lat": ds.lat, "lon": ds.lon}
    )
    out = ds.copy()
    for v in variables:
        out[v] = ds[v].where(valid_da)
    return out


def inpaint_nearest(arr: np.ndarray, max_dist_px: int) -> np.ndarray:
    """Fill NaNs from the nearest value within ``max_dist_px`` pixels."""
    invalid = np.isnan(arr)
    if max_dist_px <= 0 or not invalid.any() or invalid.all():
        return arr

    # Distance to, and index of, the nearest valid cell.
    dist, (iy, ix) = distance_transform_edt(invalid, return_indices=True)
    fill = invalid & (dist <= max_dist_px)
    if not fill.any():
        return arr

    out = arr.copy()
    out[fill] = arr[iy[fill], ix[fill]]
    return out
