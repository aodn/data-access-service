# Use the official Python base image
FROM python:3.10-slim

ENV GUNICORN_WORKERS_NUM=4
ENV GUNICORN_TIMEOUT=3600

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock ./

# For Docker build to understand the possible env
RUN apt update && \
    apt -y upgrade && \
    pip3 install --upgrade pip && \
    pip3 install poetry && \
    poetry config virtualenvs.create false && \
    poetry lock && \
    poetry install

# Expose the port the app runs on
EXPOSE 8000

# Copy the rest of the application code into the container
COPY . /app

# Run the Flask app using Poetry
CMD exec poetry run gunicorn \
    --bind 0.0.0.0:8000 \
    --worker-class gevent \
    --timeout $GUNICORN_TIMEOUT \
    --workers $GUNICORN_WORKERS_NUM \
    data_access_service.run:app
