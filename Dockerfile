FROM python:3.10-slim

WORKDIR /app
RUN useradd -l -m -s /bin/bash appuser

COPY pyproject.toml poetry.lock README.md entry_point.py /app/
COPY data_access_service /app/data_access_service

# For Docker build to understand the possible env
RUN apt update && \
    apt -y upgrade && \
    pip3 install --upgrade pip && \
    pip3 install virtualenv==20.28.1 && \
    pip3 install poetry==2.0.1 && \
    poetry config virtualenvs.create false && \
    poetry lock && \
    poetry install --no-root


RUN chown -R appuser:appuser /app
USER appuser

COPY log_config.yaml /app/log_config.yaml

EXPOSE 8000
