"""Geo helpers for the renderers."""

import math

import xarray as xr


def json_safe_float(v) -> float | None:
    """float(v), or None if NaN or infinite."""
    f = float(v)
    return None if math.isnan(f) or math.isinf(f) else f


def dataset_bounds(ds: xr.Dataset) -> tuple[float, float, float, float]:
    """(lon_min, lon_max, lat_min, lat_max) of ``ds``."""
    return (
        float(ds.lon.min().values),
        float(ds.lon.max().values),
        float(ds.lat.min().values),
        float(ds.lat.max().values),
    )
