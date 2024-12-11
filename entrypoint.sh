#!/bin/sh

poetry run gunicorn --bind 0.0.0.0:8000 --worker-class=gevent --timeout=3600 --workers=4 data_access_service.run:app
