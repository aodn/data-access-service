"""Routes over the live collection-mapping table in the data-index S3 Tables bucket.

Given a drawn area (a GeoJSON Polygon or MultiPolygon) and a UTC time window,
these endpoints return the index rows whose spatial box intersects the area and
whose ``time_coverage_start``/``time_coverage_end`` overlaps the window.

Two shapes for the same query:

* ``GET  /data-index/collection-mapping`` - polygon as a GeoJSON string in the
  ``polygon`` query param, times in ``start_time`` / ``end_time``. Convenient
  for small areas.
* ``POST /data-index/collection-mapping`` - polygon as a JSON body, for areas
  too large to sit comfortably in a URL.
"""

import json
import logging
from http import HTTPStatus
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from data_access_service.core.data_index import query_collection_mapping
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import verify_datatime_param
from data_access_service.utils.multi_polygon_helper import (
    as_one_geometry,
    merge_polygons,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# A drawn area that selects nothing is a client mistake, not an empty result -
# surface it so the caller can fix the request.
_EMPTY_AREA_DETAIL = "polygon encloses no area"
# Hard cap so a huge window over the whole globe cannot stream back the whole
# table. The caller can override with a smaller limit, never a larger one.
_MAX_LIMIT = 10000


class CollectionMappingQuery(BaseModel):
    """POST body for the collection-mapping query.

    ``polygon`` is a GeoJSON geometry. A ``Polygon`` is accepted and wrapped
    into a single-part ``MultiPolygon``; a ``MultiPolygon`` is used as is.
    ``start_time`` / ``end_time`` are ISO-8601 UTC instants; either may be
    omitted to leave that side of the window open.
    """

    polygon: dict[str, Any] = Field(
        ..., description="GeoJSON Polygon or MultiPolygon geometry"
    )
    start_time: Optional[str] = Field(
        None, description="ISO-8601 UTC start, e.g. 2024-01-01T00:00:00Z"
    )
    end_time: Optional[str] = Field(
        None, description="ISO-8601 UTC end, e.g. 2024-01-02T00:00:00Z"
    )
    limit: Optional[int] = Field(
        None, ge=1, description=f"Max rows to return (hard cap {_MAX_LIMIT})"
    )


def _to_multipolygon_dict(geometry: dict[str, Any]) -> dict[str, Any]:
    """Normalise a GeoJSON Polygon/MultiPolygon dict to a MultiPolygon dict.

    merge_polygons only understands MultiPolygon; a single Polygon is the common
    case from a one-off box, so wrap it rather than reject it.
    """
    if not isinstance(geometry, dict):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="polygon must be a GeoJSON geometry object",
        )
    geom_type = geometry.get("type")
    if geom_type == "MultiPolygon":
        return geometry
    if geom_type == "Polygon":
        return {"type": "MultiPolygon", "coordinates": [geometry.get("coordinates")]}
    raise HTTPException(
        status_code=HTTPStatus.BAD_REQUEST,
        detail=f"polygon must be a GeoJSON Polygon or MultiPolygon, got {geom_type!r}",
    )


def _resolve_geometry(polygon: dict[str, Any]):
    """Merge the drawn polygon into one geometry, or 400 if it selects no area."""
    multipolygon = _to_multipolygon_dict(polygon)
    try:
        merged = merge_polygons(multipolygon)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"invalid polygon: {exc}",
        )
    geometry = as_one_geometry(merged)
    if geometry is None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail=_EMPTY_AREA_DETAIL
        )
    return geometry


def _resolve_limit(limit: Optional[int]) -> int:
    """Clamp a requested limit to the hard cap; default to the cap when unset."""
    if limit is None or limit > _MAX_LIMIT:
        return _MAX_LIMIT
    return limit


async def _run_query(
    geometry, start_time, end_time, limit: Optional[int]
) -> dict[str, Any]:
    start_ts = verify_datatime_param("start_time", start_time) if start_time else None
    end_ts = verify_datatime_param("end_time", end_time) if end_time else None

    resolved_limit = _resolve_limit(limit)
    try:
        # The DuckDB read is blocking; keep the event loop free for other requests.
        rows = await run_in_threadpool(
            query_collection_mapping,
            geometry=geometry,
            start_time=start_ts,
            end_time=end_ts,
            limit=resolved_limit,
        )
    except Exception as exc:  # pragma: no cover - surfaced as 500 with context
        logger.error("collection-mapping query failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="Failed to query the collection-mapping table.",
        )

    return {"count": len(rows), "limit": resolved_limit, "results": rows}


@router.get("/data-index/collection-mapping", dependencies=[Depends(api_key_auth)])
async def query_collection_mapping_get(
    request: Request,
    polygon: str = Query(
        ..., description="GeoJSON Polygon or MultiPolygon as a JSON string"
    ),
    start_time: Optional[str] = Query(
        None, description="ISO-8601 UTC start, e.g. 2024-01-01T00:00:00Z"
    ),
    end_time: Optional[str] = Query(
        None, description="ISO-8601 UTC end, e.g. 2024-01-02T00:00:00Z"
    ),
    limit: Optional[int] = Query(None, ge=1),
):
    """Find index rows intersecting ``polygon`` and overlapping the time window."""
    try:
        polygon_dict = json.loads(polygon)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"polygon is not valid JSON: {exc}",
        )
    geometry = _resolve_geometry(polygon_dict)
    return await _run_query(geometry, start_time, end_time, limit)


@router.post("/data-index/collection-mapping", dependencies=[Depends(api_key_auth)])
async def query_collection_mapping_post(body: CollectionMappingQuery):
    """Same query as the GET, with the polygon in a JSON body for large areas."""
    geometry = _resolve_geometry(body.polygon)
    return await _run_query(geometry, body.start_time, body.end_time, body.limit)
