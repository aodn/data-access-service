"""Build frontend GeoJSON shapes from query results.

No I/O and no DuckDB — every function here takes a pandas ``DataFrame`` (the
output of a repository read) and returns the GeoJSON ``dict`` the frontend
expects (Highcharts series + Mapbox GL points), typed by the shapes in
:mod:`schemas`. Serialization is the caller's job (FastAPI handles it).

Two shapes, mirroring the two reads every repository exposes:

  * :func:`site_feature_collection` — for ``sites_in_date_range``. One
    ``Feature`` per site: a point at its latest LON/LAT, carrying the site id
    and date. Returns a :class:`~schemas.SiteFeatureCollection`.

  * :func:`site_details_feature_collection` — for ``site_details``. A single
    ``Feature`` for the site; each ``value_columns`` entry becomes a
    ``[time_ms, value]`` series. With a ``group_column`` (mooring:
    ``NOMINAL_DEPTH``) the series are nested per group value under a
    ``"<group_column>_<value>"`` key; without one (wave buoy: no depth
    dimension) they sit directly in ``properties``. Returns a single
    :class:`~schemas.SiteDetailsFeature`.

``time_ms`` is integer milliseconds since the epoch (UTC) — what Highcharts
wants on a datetime axis. ``coordinates`` are ``[lon, lat]``, per the GeoJSON
spec and Mapbox GL.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from data_access_service.schemas.sites import (
    SeriesByColumn,
    SiteDetailsFeature,
    SiteDetailsProperties,
    SiteFeature,
    SiteFeatureCollection,
    SitePoint,
)


def _native(value: Any) -> Any:
    """Convert a numpy/pandas scalar to a plain Python scalar (JSON-safe)."""
    return value.item() if hasattr(value, "item") else value


def _iso(value: Any) -> str:
    """Render a timestamp as an ISO 8601 string (frontend ``date`` field)."""
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else str(value)


def _point(longitude: Any, latitude: Any) -> SitePoint:
    """A GeoJSON ``Point`` from a lon/lat pair, both made JSON-safe."""
    return {
        "type": "Point",
        "coordinates": [_native(longitude), _native(latitude)],
    }


def _series(group, value_columns: Iterable[str], time_column: str) -> SeriesByColumn:
    """One ``{column: [[time_ms, value], ...]}`` dict for a frame.

    ``time_ms`` is integer milliseconds since the epoch (UTC); NULL values are
    dropped per column.
    """
    # Cast to ms resolution first, then to int — DuckDB's ``.df()`` yields
    # ``datetime64[us]``, so a plain ``astype("int64")`` would be microseconds.
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
    """Shape ``sites_in_date_range`` rows into a ``SiteFeatureCollection``.

    ``rows`` is a DataFrame with one row per site — the output of
    :meth:`~repository.ParquetRepository.sites_in_date_range`, whose columns are
    the site identifier plus the aliased ``latitude`` / ``longitude`` /
    ``time`` of each site's most recent record.

    Produces one ``Feature`` per site: a ``Point`` at its latest LON/LAT with
    ``properties = {date, site}``. The site id is always exposed under ``site``
    regardless of which column it came from (``site_column``), since that's the
    field the frontend ``SiteProperties`` reads for both products. ``_id`` is
    intentionally omitted — the frontend injects it, it isn't part of the API
    payload.
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


def site_details_feature_collection(
    rows,
    *,
    time_column: str,
    longitude_column: str,
    latitude_column: str,
    value_columns: Iterable[str],
    group_column: str | None = None,
) -> SiteDetailsFeature:
    """Shape ``site_observations`` rows into a single details ``Feature``.

    A pure data transform — no I/O. ``rows`` is a DataFrame holding the time,
    location, the requested ``value_columns`` and (for mooring) the
    ``group_column``, ordered by group then time.

    The site is a single ``Feature`` (a ``Point`` at its LON/LAT). Each
    ``value_columns`` entry becomes a ``[time_ms, value]`` series — NULLs
    dropped — where ``time_ms`` is integer milliseconds since epoch (UTC,
    Highcharts-ready).

    * With ``group_column`` (mooring ``NOMINAL_DEPTH``): ``properties`` nest one
      series object per distinct group value, keyed ``"<group_column>_<value>"``
      (e.g. ``"NOMINAL_DEPTH_10"``).
    * Without ``group_column`` (wave buoy, no depth dimension): the series sit
      directly in ``properties``.

    An empty frame yields a ``Feature`` with empty ``properties`` and ``null``
    geometry (there is no location to place it).
    """
    value_columns = [c for c in value_columns if c != group_column]

    if len(rows) == 0:
        return {"type": "Feature", "properties": {}, "geometry": None}

    properties: SiteDetailsProperties
    if group_column is not None:
        # rows arrive ordered by group then time, so each group is already
        # time-sorted.
        properties = {
            f"{group_column}_{_native(key)}": _series(group, value_columns, time_column)
            for key, group in rows.groupby(group_column, sort=True)
        }
    else:
        properties = _series(rows, value_columns, time_column)

    feature: SiteDetailsFeature = {
        "type": "Feature",
        "properties": properties,
        "geometry": _point(
            rows[longitude_column].iloc[0],
            rows[latitude_column].iloc[0],
        ),
    }
    return feature
