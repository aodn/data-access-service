"""Turn repository DataFrames into the GeoJSON the frontend reads.

Pure transforms, no I/O. Shapes are typed in :mod:`sites`.

  * :func:`site_feature_collection` — ``sites_in_date_range`` rows, one point per site.
  * :func:`site_details_feature_collection` — ``site_details`` rows, one Feature
    with a ``[time_ms, value]`` series per value column, nested by any group columns.

``time_ms`` is UTC epoch milliseconds (Highcharts); ``coordinates`` are ``[lon, lat]``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from data_access_service.sites.sites import (
    SeriesByColumn,
    SiteDetailsFeature,
    SiteDetailsProperties,
    SiteFeature,
    SiteFeatureCollection,
    SitePoint,
)
from data_access_service.utils.date_time_utils import to_utc_iso_z


def _native(value: Any) -> Any:
    """numpy/pandas scalar to a plain Python value."""
    return value.item() if hasattr(value, "item") else value


def _group_key(value: Any) -> Any:
    """Group value for a key; floats rounded so float32 0.9 isn't 0.8999999761581421."""
    value = _native(value)
    return round(value, 3) if isinstance(value, float) else value


def _iso(value: Any) -> str:
    """Timestamp to ISO 8601 UTC, e.g. "2024-01-15T00:00:00Z" (naive is read as UTC).

    Strings pass through; other non-timestamps fall back to ``str()``.
    """
    if isinstance(value, str):
        return value
    try:
        return to_utc_iso_z(value)
    except (TypeError, ValueError):
        return str(value)


def _point(longitude: Any, latitude: Any) -> SitePoint:
    """GeoJSON ``Point`` from lon/lat."""
    return {
        "type": "Point",
        "coordinates": [_native(longitude), _native(latitude)],
    }


def _series(group, value_columns: Iterable[str], time_column: str) -> SeriesByColumn:
    """``{column: [[time_ms, value], ...]}`` for a frame, NULLs dropped per column."""
    # .df() gives datetime64[us]; cast to ms before int64.
    time_ms = group[time_column].astype("datetime64[ms]").astype("int64")
    series: SeriesByColumn = {}
    for col in value_columns:
        present = group[col].notna()
        series[col] = [
            (int(ms), _native(value))
            for ms, value in zip(time_ms[present], group[col][present], strict=True)
        ]
    return series


def site_feature_collection(
    rows,
    *,
    site_column: str,
    time_column: str = "time",
    longitude_column: str = "longitude",
    latitude_column: str = "latitude",
) -> SiteFeatureCollection:
    """One ``Feature`` per site at its latest location, ``properties = {date, site}``.

    ``rows`` comes from ``ParquetRepository.sites_in_date_range``. The id is always
    under ``site`` whatever ``site_column`` is; ``_id`` is left for the frontend.
    """
    features: list[SiteFeature] = []
    for row in rows.itertuples(index=False):
        site = _native(getattr(row, site_column))
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "date": _iso(getattr(row, time_column)),
                    "site": site,
                },
                "geometry": _point(
                    getattr(row, longitude_column),
                    getattr(row, latitude_column),
                ),
            }
        )

    return {"type": "FeatureCollection", "features": features}


def _nested_series(
    rows, group_columns: Sequence[str], value_columns: list[str], time_column: str
) -> SiteDetailsProperties:
    """Series for ``rows``, nested one level per group column (none: flat)."""
    if not group_columns:
        return _series(rows, value_columns, time_column)
    column, rest = group_columns[0], group_columns[1:]
    # rows are sorted by group then time, so each group is already time-sorted.
    return {
        f"{column}_{_group_key(key)}": _nested_series(
            group, rest, value_columns, time_column
        )
        for key, group in rows.groupby(column, sort=True)
    }


def site_details_feature_collection(
    rows,
    *,
    time_column: str,
    longitude_column: str,
    latitude_column: str,
    value_columns: Iterable[str],
    group_columns: Sequence[str] = (),
) -> SiteDetailsFeature:
    """One site's ``site_details`` rows as a single ``Feature``.

    ``properties`` nest one level per group column, keyed ``"<column>_<value>"``
    (mooring: ``NOMINAL_DEPTH_10``; estuarine: ``instrument_EXO2-SONDE`` ->
    ``NOMINAL_DEPTH_0.9``). With no group columns (wave buoy) the series sit
    directly in ``properties``. No rows gives empty ``properties`` and null geometry.
    """
    value_columns = [c for c in value_columns if c not in group_columns]

    if len(rows) == 0:
        return {"type": "Feature", "properties": {}, "geometry": None}

    properties = _nested_series(rows, group_columns, value_columns, time_column)

    feature: SiteDetailsFeature = {
        "type": "Feature",
        "properties": properties,
        "geometry": _point(
            rows[longitude_column].iloc[0],
            rows[latitude_column].iloc[0],
        ),
    }
    return feature
