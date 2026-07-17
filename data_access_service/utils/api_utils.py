import asyncio
import logging

from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

from data_access_service import Config
from data_access_service.core.api import API

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key")


async def api_key_auth(x_api_key: str = Security(api_key_header)):
    if x_api_key == Config.get_config().get_api_key():
        return x_api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )


async def wait_until_api_ready(api: API, timeout: float = 300) -> None:
    """Poll api.get_api_status() until it's ready or timeout elapses.

    Used to hold off other CPU/memory-heavy startup work (repository refresh,
    tiler startup) until metadata init has finished, so it isn't competing
    with them for resources.
    """
    waited = 0.0
    while not api.get_api_status():
        if waited >= timeout:
            logger.warning("Timed out waiting for API to become ready")
            break
        await asyncio.sleep(0.5)
        waited += 0.5
    logger.info(f"API ready status = {api.get_api_status()} (waited {waited}s)")
