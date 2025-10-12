FROM python:3.10-slim

WORKDIR /app
RUN useradd -l -m -s /bin/bash appuser

COPY pyproject.toml poetry.lock README.md entry_point.py /app/
COPY data_access_service /app/data_access_service
COPY log_config.yaml /app/log_config.yaml

# For Docker build to understand the possible env
# nginx to allow offload of health check on busy python app to get fast response
# supervisor use to start mutliple process
# netcat-openbsd this image allow nginx to probe status correctly
RUN apt update && \
    apt -y upgrade && \
    apt install -y nginx supervisor netcat-openbsd && \
    pip3 install --upgrade pip && \
    pip3 install virtualenv==20.28.1 && \
    pip3 install poetry && \
    poetry config virtualenvs.create false && \
    poetry lock && \
    poetry install --no-root

COPY das_site.conf  /etc/nginx/sites-available/
RUN ln -s /etc/nginx/sites-available/das_site.conf /etc/nginx/sites-enabled/ \
    && rm /etc/nginx/sites-enabled/default \
    && nginx -t  # Test config during build

# Copy Supervisor config to run both services
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN chown -R appuser:appuser /app /var/log/nginx/error.log /var/log/nginx/access.log /run/nginx.pid
USER appuser

EXPOSE 8000

# Run Supervisor as the main process, it starts an nginx server and our app. In case the app die, it
# will restart the app. The ngnix is use to block too many request and offload the health check so
# that we always get response even the app is busy. User who do not want this behavior should
# use their own entry point to start the app
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
