import logging
import os

from flask import Flask

from data_access_service.config.config import (
    EnvType,
    DevConfig,
    StagingConfig,
    EdgeConfig,
    ProdConfig,
)
from data_access_service.core.api import API

app = Flask(__name__)


def create_app():
    # Load configuration
    profile = EnvType(os.getenv("PROFILE", EnvType.DEV))

    if profile == EnvType.PRODUCTION:
        app.config.from_object(ProdConfig)
    elif profile == EnvType.EDGE:
        app.config.from_object(EdgeConfig)
    elif profile == EnvType.STAGING:
        app.config.from_object(StagingConfig)
    else:
        app.config.from_object(DevConfig)

    logging.basicConfig(
        level=app.config["LOGLEVEL"],
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"Environment profile is {profile}")

    # Register the Blueprint with a URL prefix
    from data_access_service.core.restapi import restapi

    app.register_blueprint(restapi, url_prefix=app.config["BASE_URL"])
    app.api = API()

    return app
