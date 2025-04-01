FROM python:3.10-slim

WORKDIR /app
RUN useradd -l -m -s /bin/bash appuser

COPY pyproject.toml poetry.lock README.md ./
COPY data_access_service ./data_access_service

# For Docker build to understand the possible env
RUN apt update && \
    apt -y upgrade && \
    pip3 install --upgrade pip && \
    pip3 install poetry==1.8.5 && \
    poetry config virtualenvs.create false && \
    poetry lock && \
    poetry install --no-root


COPY . /app

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
