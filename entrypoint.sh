#!/bin/sh

poetry run gunicorn --bind 0.0.0.0:8000 --worker-class gevent --timeout "${GUNICORN_TIMEOUT:-3600}" --workers "${GUNICORN_WORKERS_NUM:-4}" data_access_service.run:app
