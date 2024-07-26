from flask import Flask, g
import logging


def create_app():
    app = Flask(__name__)

    # Load configuration
    app.config.from_object('config.Config')

    # Register the Blueprint with a URL prefix
    from .restapi import restapi
    app.register_blueprint(restapi, url_prefix='/api/v1/das')

    # Create a logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    return app
