"""Service for the ``/data/feature-collection`` routes.

Parses dates, calls the repository and builds the GeoJSON.

One instance per request, wrapping one product's repository (see ``get_site_service``
in ``core/routes/helpers.py``).
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
from data_access_service.core.routes.helpers import parse_utc_datetime


class SiteFeatureService:
    """Build the site feature-collection payloads for one product's repository."""

    def __init__(self, repo: ParquetRepository) -> None:
        self._repo = repo

    def sites_with_data_between(
        self, start_date: str | None, end_date: str | None
    ) -> SiteFeatureCollection:
        """Sites with data in ``[start_date, end_date]``; both optional ISO 8601, read as UTC."""
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
        """One site's series as a details ``Feature``, nested by the repo's ``group_columns``.

        ``start_date`` / ``end_date`` are optional ISO 8601, read as UTC.
        """
        start = parse_utc_datetime("start_date", start_date)
        end = parse_utc_datetime("end_date", end_date)
        return site_details_feature_collection(
            self._repo.site_details(site, start, end),
            time_column=self._repo.time_column,
            longitude_column=self._repo.longitude_column,
            latitude_column=self._repo.latitude_column,
            value_columns=list(self._repo.value_columns),
            group_columns=self._repo.group_columns,
        )
