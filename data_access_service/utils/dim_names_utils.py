"""Resolve a dataset key's latitude / longitude / time dimension names.
"""

from data_access_service.core.constants import (
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)


def resolve_dim_names(
    api, uuid: str, key: str
) -> tuple[str | None, str | None, str | None]:
    """Return (latitude, longitude, time) dimension names for `key`, from the
    dataset's own metadata (api.map_column_names). Any axis the dataset does not
    define comes back as None."""

    def _one(column: str) -> str | None:
        mapped = api.map_column_names(uuid, key, [column]) or []
        return mapped[0] if mapped else None

    return (
        _one(STR_LATITUDE_UPPER_CASE),
        _one(STR_LONGITUDE_UPPER_CASE),
        _one(STR_TIME_UPPER_CASE),
    )
