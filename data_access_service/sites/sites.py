"""Response shapes for the API — the JSON the endpoints return.

Named ``TypedDict`` / alias shapes shared by :mod:`api` (FastAPI response
models) and :mod:`geojson` (the functions that build them), so neither
returns a bare ``dict[str, Any]``. They describe *structure* only — a point, a
series map, a feature collection — and deliberately don't know about products or
measurement names, since those keys are chosen by the caller at runtime.

These are imported by FastAPI as response models, so the ``TypedDict`` comes
from ``typing_extensions``: pydantic v2 rejects ``typing.TypedDict`` on Python
< 3.12.
"""

from __future__ import annotations

from typing import Literal

from typing_extensions import TypedDict


class SitePoint(TypedDict):
    """A GeoJSON ``Point``; ``coordinates`` are ``[lon, lat]``."""

    type: Literal["Point"]
    coordinates: list[float]


# One column's ``[time_ms, value]`` points, and the ``{column: points}`` map
# that ``geojson._series`` builds for a frame.
SiteSeries = list[tuple[int, float]]
SeriesByColumn = dict[str, SiteSeries]


# --- /sites ------------------------------------------------------------------


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


# --- /sites/{site}/details ---------------------------------------------------

# Details properties are either the series map directly (ungrouped) or one such
# map per group key (grouped) — the two branches of
# ``geojson.site_details_feature_collection``.
SiteDetailsProperties = SeriesByColumn | dict[str, SeriesByColumn]


class SiteDetailsFeature(TypedDict):
    type: Literal["Feature"]
    properties: SiteDetailsProperties
    # ``None`` only for the empty-frame case, where there is no LON/LAT to place.
    geometry: SitePoint | None


# --- /schema and /latest-time ------------------------------------------------


class SchemaColumn(TypedDict):
    """One row of the ``/schema`` read — a column and its DuckDB type."""

    column_name: str
    column_type: str


class LatestTime(TypedDict):
    """The ``/latest-time`` read; ``time`` is ISO 8601, or ``None`` if empty."""

    time: str | None
