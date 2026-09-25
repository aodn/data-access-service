"""Manual trigger for the tiler refresh the cron job runs."""

from http import HTTPStatus

import anyio
from fastapi import APIRouter, HTTPException

from data_access_service.core.tiler_routes.startup import (
    RefreshInProgressError,
    refresh_tiler,
)

router = APIRouter()


@router.post(
    "/refresh",
    summary="Refresh tiler stores",
    description=(
        "Re-reads every store's metadata.json and root_metadata.json so the "
        "tiler serves the latest data without waiting for the scheduled refresh."
    ),
)
async def refresh():
    try:
        products, outcomes = await anyio.to_thread.run_sync(refresh_tiler)
    except RefreshInProgressError as e:
        raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(e))
    return {
        "products": len(products),
        "stores": len(outcomes),
        "failed_stores": sorted(s for s, err in outcomes.items() if err is not None),
    }
