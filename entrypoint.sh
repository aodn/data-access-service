#!/bin/sh

poetry run uvicorn --bind 0.0.0.0:8000 --worker-class=gevent --timeout=3600 --workers=4 --reload --log-config=log_config.yaml data_access_service.server:app
