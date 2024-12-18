import logging
import dataclasses
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query, Request

from data_access_service.common.settings import (
    Config,
)
from data_access_service.core.api import API
from data_access_service.common.utils import (
    response_netcdf,
    response_json,
    verify_depth_param,
    verify_datatime_param,
)


# Create an instance of the API class
api_instance = API()
router = APIRouter(prefix=Config.BASE_URL)
log = logging.getLogger(__name__)


@router.get("/hello")
async def hello():
    return {"content": "Hello World!"}


@router.get("/metadata/{uuid}")
def get_mapped_metadata(uuid: str):
    return dataclasses.asdict(api_instance.get_mapped_meta_data(uuid))


@router.get("/metadata/{uuid}/raw")
async def get_raw_metadata(uuid: str):
    raw_metadata = api_instance.get_raw_meta_data(uuid)
    if raw_metadata is None:
        return {"error": "Metadata not found"}, 404
    return raw_metadata


@router.get("/data/{uuid}")
async def get_data(
    request: Request,
    uuid: str,
    start_date: str = Query(None),
    end_date: str = Query(None),
    start_depth: float = Query(default=None),
    end_depth: float = Query(default=None),
    f: str = Query(default="json"),
):

    start_date = verify_datatime_param("start_date", start_date)
    end_date = verify_datatime_param("end_date", end_date)

    result: Optional[pd.DataFrame] = api_instance.get_dataset_data(
        uuid=uuid, date_start=start_date, date_end=end_date
    )

    start_depth = verify_depth_param("start_depth", start_depth)
    end_depth = verify_depth_param("end_depth", end_depth)

    # The cloud optimized format is fast to lookup if there is an index, some field isn't part of the
    # index and therefore will not gain to filter by those field, indexed fields are site_code, timestamp, polygon

    # Depth is below sea level zero, so logic slightly diff
    if start_depth is not None and end_depth is not None:
        filtered = result[
            (result["DEPTH"] <= start_depth) & (result["DEPTH"] >= end_depth)
        ]
    elif start_depth is not None:
        filtered = result[(result["DEPTH"] <= start_depth)]
    elif end_depth is not None:
        filtered = result[result["DEPTH"] >= end_depth]
    else:
        filtered = result

    log.info("Record number return %s for query", len(filtered.index))

    if f == "json":
        # Depends on whether receiver support gzip encoding
        compress = "gzip" in request.headers.get("Accept-Encoding", "")
        return response_json(filtered, compress)
    elif f == "netcdf":
        return response_netcdf(filtered)
