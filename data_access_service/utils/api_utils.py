import os

from dotenv import load_dotenv
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

from data_access_service import Config

api_key_header = APIKeyHeader(name="X-API-Key")


async def api_key_auth(x_api_key: str = Security(api_key_header)):
    if x_api_key == Config.get_config().get_api_key():
        return x_api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )
