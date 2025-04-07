from data_access_service import logger
from data_access_service.core.api import API
from data_access_service.utils.api_utils import api_key_auth


import dataclasses
from typing import Optional, List
from datetime import datetime
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import numpy as np
import json

api_instance = API()
API_PREFIX = "/api/v1/das"
router = APIRouter(prefix=API_PREFIX)

RECORD_PER_PARTITION: Optional[int] = 1000
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


class FormattedMetadataRequest(BaseModel):
    uuid: str

class RawMatadataRequest(BaseModel):
    uuid: str

class NotebookUrlRequest(BaseModel):
    uuid: str

class HasDataRequest(BaseModel):
    uuid: str

class GetTemporalExtentRequest(BaseModel):
    uuid: str

class GetDataRequest(BaseModel):
    uuid: str
    start_date: datetime
    end_date: datetime
    format: str
    columns: Optional[List[str]] = None
    start_depth: float
    end_depth: float
    is_to_index: Optional[bool] = None

class HealthCheckResponse(BaseModel):
    status: str

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """
    Health check endpoint.
    """
    return HealthCheckResponse(status="healthy")


@router.get("/metadata/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_mapped_metadata(uuid: str):
    if uuid is not None:
        return dataclasses.asdict(api_instance.get_mapped_meta_data(uuid))
    else:
        return list(api_instance.get_mapped_meta_data(None))

@router.get("/metadata/{uuid}/raw", dependencies=[Depends(api_key_auth)])
async def get_raw_metadata(uuid: str):
    return api_instance.get_raw_meta_data(uuid)


@router.get("/data/{uuid}/notebook_url", dependencies=[Depends(api_key_auth)])
async def get_notebook_url(uuid: str):
    i = api_instance.get_notebook_from(uuid)
    if isinstance(i, ValueError):
        raise HTTPException(status_code=404, detail="Notebook URL not found")
    return i


@router.get("/data/{uuid}/has_data", dependencies=[Depends(api_key_auth)])
async def has_data(uuid: str):
    # TOODO
    pass


@router.get("/data/{uuid}/temporal_extent", dependencies=[Depends(api_key_auth)])
async def get_temporal_extent(uuid: str):
    # TODO
    pass


@router.get("/data/{uuid}", dependencies=[Depends(api_key_auth)])
async def get_data(uuid: str):
    # TODO
    pass