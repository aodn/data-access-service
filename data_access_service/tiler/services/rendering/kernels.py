"""Numba kernels (with an xarray fallback) to resample a slice to the LOD
grid and normalise it to integers."""

import logging
import threading

import numpy as np
import xarray as xr

from data_access_service.tiler.services.colormap.categorical import (
    is_categorical_variable,
)

logger = logging.getLogger(__name__)

# Numba's default threading layer isn't thread-safe: two parallel kernels
# running at once can corrupt output or crash. Held per kernel call.
_PARALLEL_KERNEL_LOCK = threading.Lock()


try:
    from numba import njit, prange

    _HAS_NUMBA = True

    # fastmath is fine: NaN still propagates through the arithmetic.
    @njit(parallel=True, cache=True, fastmath=True)
    def _numba_bilinear(src: np.ndarray, total_h: int, total_w: int) -> np.ndarray:
        """Bilinear resample of ``src`` (north to south) to (total_h, total_w).
        NaN if any of the 4 neighbours is NaN."""
        src_h, src_w = src.shape
        out = np.empty((total_h, total_w), dtype=np.float32)
        sy_scale = (src_h - 1.0) / (total_h - 1.0) if total_h > 1 else 0.0
        sx_scale = (src_w - 1.0) / (total_w - 1.0) if total_w > 1 else 0.0
        for i in prange(total_h):
            sy = i * sy_scale
            y0 = int(sy)
            y1 = y0 + 1 if y0 + 1 < src_h else src_h - 1
            dy = sy - y0
            for j in range(total_w):
                sx = j * sx_scale
                x0 = int(sx)
                x1 = x0 + 1 if x0 + 1 < src_w else src_w - 1
                dx = sx - x0
                a = src[y0, x0]
                b = src[y0, x1]
                c = src[y1, x0]
                d = src[y1, x1]
                if np.isnan(a) or np.isnan(b) or np.isnan(c) or np.isnan(d):
                    out[i, j] = np.nan
                else:
                    top = a * (1.0 - dx) + b * dx
                    bot = c * (1.0 - dx) + d * dx
                    out[i, j] = top * (1.0 - dy) + bot * dy
        return out

    @njit(parallel=True, cache=True, fastmath=True)
    def _numba_nearest(src: np.ndarray, total_h: int, total_w: int) -> np.ndarray:
        """Nearest-neighbour resample, for categorical variables, so codes are
        never blended."""
        src_h, src_w = src.shape
        out = np.empty((total_h, total_w), dtype=np.float32)
        sy_scale = (src_h - 1.0) / (total_h - 1.0) if total_h > 1 else 0.0
        sx_scale = (src_w - 1.0) / (total_w - 1.0) if total_w > 1 else 0.0
        for i in prange(total_h):
            y = int(i * sy_scale + 0.5)
            if y >= src_h:
                y = src_h - 1
            for j in range(total_w):
                x = int(j * sx_scale + 0.5)
                if x >= src_w:
                    x = src_w - 1
                out[i, j] = src[y, x]
        return out

    # No 'nnan' in fastmath, so np.isnan works here.
    @njit(
        parallel=True,
        cache=True,
        fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    )
    def _numba_normalize_uint32(
        arr: np.ndarray, lo: float, hi: float, out_max: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """float32 -> uint32, plus a valid mask (1 where not NaN)."""
        h, w = arr.shape
        out = np.empty((h, w), dtype=np.uint32)
        valid = np.empty((h, w), dtype=np.uint8)
        span = hi - lo if hi != lo else 1.0
        scale = (1.0 / span) * out_max
        out_max_f = float(out_max)
        for i in prange(h):
            for j in range(w):
                v = arr[i, j]
                if np.isnan(v):
                    out[i, j] = np.uint32(0)
                    valid[i, j] = np.uint8(0)
                else:
                    val = (v - lo) * scale
                    if val < 0.0:
                        val = 0.0
                    elif val > out_max_f:
                        val = out_max_f
                    out[i, j] = np.uint32(val)
                    valid[i, j] = np.uint8(1)
        return out, valid

    @njit(
        parallel=True,
        cache=True,
        fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    )
    def _numba_normalize_uint8(
        arr: np.ndarray, lo: float, hi: float, out_max: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """The uint8 version, for variable pairs."""
        h, w = arr.shape
        out = np.empty((h, w), dtype=np.uint8)
        valid = np.empty((h, w), dtype=np.uint8)
        span = hi - lo if hi != lo else 1.0
        scale = (1.0 / span) * out_max
        out_max_f = float(out_max)
        for i in prange(h):
            for j in range(w):
                v = arr[i, j]
                if np.isnan(v):
                    out[i, j] = np.uint8(0)
                    valid[i, j] = np.uint8(0)
                else:
                    val = (v - lo) * scale
                    if val < 0.0:
                        val = 0.0
                    elif val > out_max_f:
                        val = out_max_f
                    out[i, j] = np.uint8(val)
                    valid[i, j] = np.uint8(1)
        return out, valid

except ImportError:  # pragma: no cover
    _HAS_NUMBA = False
    logger.warning("numba unavailable; falling back to xr.interp (~5× slower on Intel)")


def resample_variables_to_grid(
    ds: xr.Dataset, variables: list[str], total_w: int, total_h: int
) -> list[np.ndarray]:
    """Resample each variable to (total_h, total_w), north to south: bilinear,
    or nearest for categorical variables. Returns float32 arrays."""
    # Resample north to south.
    flip = float(ds.lat[0]) < float(ds.lat[-1])

    if _HAS_NUMBA:
        out: list[np.ndarray] = []
        for v in variables:
            arr = ds[v].values.astype(np.float32, copy=False).squeeze()
            if flip:
                arr = np.ascontiguousarray(arr[::-1, :])
            kernel = (
                _numba_nearest
                if is_categorical_variable(ds[v].attrs)
                else _numba_bilinear
            )
            with _PARALLEL_KERNEL_LOCK:
                out.append(kernel(arr, total_h, total_w))
        return out

    # Fallback: xarray interp.
    lon_min = float(ds.lon.min())
    lon_max = float(ds.lon.max())
    lat_min = float(ds.lat.min())
    lat_max = float(ds.lat.max())
    target_lons = np.linspace(lon_min, lon_max, total_w)
    target_lats = np.linspace(lat_max, lat_min, total_h)  # north to south
    out = []
    for v in variables:
        method = "nearest" if is_categorical_variable(ds[v].attrs) else "linear"
        r = ds[v].interp(lon=target_lons, lat=target_lats, method=method)
        out.append(r.values.squeeze().astype(np.float32, copy=False))
    return out


def normalize_fallback(
    arr: np.ndarray, lo: float, hi: float, out_max: int
) -> np.ndarray:
    """``arr`` scaled to [0, out_max], NaN as 0 (no-numba fallback)."""
    span = hi - lo if hi != lo else 1.0
    result = np.clip((np.nan_to_num(arr, nan=0.0) - lo) / span * out_max, 0, out_max)
    return result.astype(np.uint32 if out_max > 255 else np.uint8)


def normalize(
    arr: np.ndarray, lo: float, hi: float, out_max: int
) -> tuple[np.ndarray, np.ndarray]:
    """float32 -> uint32 (or uint8 if out_max <= 255), plus a valid mask."""
    if _HAS_NUMBA:
        with _PARALLEL_KERNEL_LOCK:
            if out_max > 255:
                return _numba_normalize_uint32(arr, lo, hi, out_max)
            return _numba_normalize_uint8(arr, lo, hi, out_max)
    norm = normalize_fallback(arr, lo, hi, out_max)
    valid = (~np.isnan(arr)).astype(np.uint8)
    return norm, valid


def warmup_resample() -> None:
    """Compile the kernels at startup, so the first tile isn't slow."""
    ds = xr.Dataset(
        {"v": (("lat", "lon"), np.zeros((16, 16), dtype=np.float32))},
        coords={"lat": np.linspace(1.0, 0.0, 16), "lon": np.linspace(0.0, 1.0, 16)},
    )
    resample_variables_to_grid(ds, ["v"], 32, 32)
    if _HAS_NUMBA:
        # No lock needed: nothing else runs yet.
        sample = np.zeros((32, 32), dtype=np.float32)
        _numba_nearest(sample, 32, 32)
        _numba_normalize_uint32(sample, 0.0, 1.0, 16777215)
        _numba_normalize_uint8(sample, 0.0, 1.0, 255)
