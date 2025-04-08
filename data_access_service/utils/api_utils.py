import os

from dotenv import load_dotenv
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

load_dotenv()

API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="X_API_Key")


async def api_key_auth(x_api_key: str = Security(api_key_header)):
    if x_api_key == API_KEY:
        return x_api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )
