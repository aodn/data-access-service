import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI

from data_access_service.common.settings import (
    Profile,
    DevProfile,
    StagingProfile,
    EdgeProfile,
    ProdProfile,
)

from data_access_service.core.routes import router as api_router


# Load environment variables from .env file
load_dotenv()
log = logging.getLogger(__name__)


def get_config():
    profile = Profile(os.getenv("PROFILE", Profile.DEV))
    log.info(f"Environment profile is {profile}")
    if profile == Profile.DEV:
        return DevProfile
    elif profile == Profile.EDGE:
        return EdgeProfile
    elif profile == Profile.STAGING:
        return StagingProfile
    elif profile == Profile.PRODUCTION:
        return ProdProfile


# Get the current config
config = get_config()
# Apply the config to the FastAPI app
app = FastAPI(debug=config.DEBUG)
# logging.basicConfig(
#     level=config.LOGLEVEL,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )


app.include_router(api_router)
