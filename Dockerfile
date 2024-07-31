# Use the official Python base image
FROM python:3.10-slim

# Install Poetry
RUN apt update
RUN yes | apt install pipx
RUN pipx ensurepath
RUN pipx install poetry

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"
ENV GUNICORN_WORKERS_NUM=4
ENV GUNICORN_TIMEOUT=3600

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock ./

# For Docker build to understand the possible env
RUN poetry env use python3.10

# Install the dependencies
RUN poetry lock
RUN poetry install --no-dev --no-interaction --no-ansi

# Copy the rest of the application code into the container
ADD data_access_service ./data_access_service
COPY config.py .
COPY run.py .

# Expose the port the app runs on
EXPOSE 8000

# Run the Flask app using Poetry
CMD exec poetry run gunicorn \
    --bind 0.0.0.0:8000 \
    --worker-class gevent \
    --timeout $GUNICORN_TIMEOUT \
    --workers $GUNICORN_WORKERS_NUM \
    run:app