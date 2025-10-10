FROM tiangolo/uwsgi-nginx:python3.10

WORKDIR /app

COPY pyproject.toml poetry.lock README.md entry_point.py /app/
COPY data_access_service /app/data_access_service

# Copy uWSGI config (required for your app; adjust path if entry_point.py handles it)
COPY uwsgi.ini /app/uwsgi.ini

# Copy Nginx custom config (gets auto-included for /health routing)
COPY custom.conf /etc/nginx/conf.d/custom.conf

# For Docker build to understand the possible env
RUN apt update && \
    apt -y upgrade && \
    pip3 install --upgrade pip && \
    pip3 install virtualenv==20.28.1 && \
    pip3 install poetry && \
    poetry config virtualenvs.create false && \
    poetry lock && \
    poetry install --no-root

# Do not need to switch user, image will use www-data automatically

# Switch to non-root user for security
COPY log_config.yaml /app/log_config.yaml

RUN chown -R www-data:www-data /app

EXPOSE 8000:80
