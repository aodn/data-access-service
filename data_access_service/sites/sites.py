"""Response shapes for the ``/data/feature-collection`` endpoints.

Used as FastAPI response models (``core/routes/data.py``) and built by :mod:`geojson`.
Structure only; product and variable names are runtime keys.

``TypedDict`` is from ``typing_extensions``: pydantic v2 rejects ``typing.TypedDict``
before Python 3.12.
"""

from __future__ import annotations

from typing import Literal

from typing_extensions import TypedDict


class SitePoint(TypedDict):
    """A GeoJSON ``Point``; ``coordinates`` are ``[lon, lat]``."""

    type: Literal["Point"]
    coordinates: list[float]


# One column's ``[time_ms, value]`` points, and the ``{column: points}`` map.
SiteSeries = list[tuple[int, float]]
SeriesByColumn = dict[str, SiteSeries]


# --- /feature-collection/{product} -------------------------------------------


class SiteProperties(TypedDict):
    date: str
    site: str | int


class SiteFeature(TypedDict):
    type: Literal["Feature"]
    properties: SiteProperties
    geometry: SitePoint


class SiteFeatureCollection(TypedDict):
    type: Literal["FeatureCollection"]
    features: list[SiteFeature]


# --- /feature-collection/{product}/{site} ------------------------------------

# Flat (wave buoy), one level (mooring: depth) or two (estuarine: instrument, depth).
# Deeper needs a new shape here and a higher sites_repository.MAX_GROUP_COLUMNS.
SiteDetailsProperties = (
    SeriesByColumn | dict[str, SeriesByColumn] | dict[str, dict[str, SeriesByColumn]]
)


class SiteDetailsFeature(TypedDict):
    type: Literal["Feature"]
    properties: SiteDetailsProperties
    # ``None`` when there are no rows.
    geometry: SitePoint | None


# --- /feature-collection/{product}/latest ------------------------------------


class SchemaColumn(TypedDict):
    """A column name and its DuckDB type (not used by any route)."""

    column_name: str
    column_type: str


class LatestTime(TypedDict):
    """Latest observation time as ISO 8601, or ``None`` if there is no data."""

    time: str | None
