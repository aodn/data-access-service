from flask import Flask, g
from .api import API
import logging

app = Flask(__name__)


def create_app():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Load configuration
    app.config.from_object('config.Config')

    # Register the Blueprint with a URL prefix
    from .restapi import restapi
    app.register_blueprint(restapi, url_prefix='/api/v1/das')
    app.api = API()

    return app
