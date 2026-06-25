"""Service layer over a :class:`ParquetRepository` for the site feature-collection endpoints.

Sits between the HTTP routes and the repository: routes own only request/response
concerns, this owns the orchestration each endpoint needs — parsing the ISO date
params and shaping repository rows into the GeoJSON payloads the frontend reads.
The repository stays a pure data-access object; the column-metadata-to-GeoJSON
plumbing lives here rather than leaking into the handlers.

One instance wraps one repository (i.e. one product); construct it per request from
the resolved repo (see ``get_site_service`` in :mod:`data_access_service.utils.routes_helper`).
"""

from __future__ import annotations

from data_access_service.sites.sites_repository import ParquetRepository
from data_access_service.sites.sites import (
    LatestTime,
    SiteDetailsFeature,
    SiteFeatureCollection,
)
from data_access_service.sites.geojson import (
    _iso,
    site_details_feature_collection,
    site_feature_collection,
)
from data_access_service.utils.routes_helper import parse_utc_datetime


class SiteFeatureService:
    """Build the site feature-collection payloads for one product's repository."""

    def __init__(self, repo: ParquetRepository) -> None:
        self._repo = repo

    def sites_with_data_between(
        self, start_date: str | None, end_date: str | None
    ) -> SiteFeatureCollection:
        """Sites with data in ``[start_date, end_date]`` as a ``SiteFeatureCollection``.

        ``start_date`` / ``end_date`` are optional ISO 8601; either may be omitted
        (omitting both returns all sites). They are normalized to naive UTC to match
        the dataset's ``TIMESTAMP_NS`` time column.
        """
        start = parse_utc_datetime("start_date", start_date)
        end = parse_utc_datetime("end_date", end_date)
        return site_feature_collection(
            self._repo.sites_in_date_range(start, end),
            site_column=self._repo.site_column,
        )

    def latest_time(self) -> LatestTime:
        """The single most recent observation time across all sites."""
        latest = self._repo.latest_time()
        return {"time": _iso(latest) if latest is not None else None}

    def site_details(
        self, site: str, start_date: str | None, end_date: str | None
    ) -> SiteDetailsFeature:
        """One site's observation timeseries as a single details ``Feature``.

        ``start_date`` / ``end_date`` are optional ISO 8601; either may be omitted
        (omitting both returns the full series). They are normalized to naive UTC to
        match the dataset's ``TIMESTAMP_NS`` time column. Each ``value_columns``
        entry becomes a ``[time_ms, value]`` series; for a grouped dataset (mooring)
        the series are nested per depth under a ``"NOMINAL_DEPTH_<value>"`` key.
        """
        start = parse_utc_datetime("start_date", start_date)
        end = parse_utc_datetime("end_date", end_date)
        return site_details_feature_collection(
            self._repo.site_details(site, start, end),
            time_column=self._repo.time_column,
            longitude_column=self._repo.longitude_column,
            latitude_column=self._repo.latitude_column,
            value_columns=list(self._repo.value_columns),
            group_column=self._repo.group_column,
        )
