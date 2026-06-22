FROM mambaorg/micromamba:2 AS conda-base
# Using micromamba (a fast alternative to conda) to build the exact environment

USER root
WORKDIR /app
RUN useradd -l -m -s /bin/bash appuser

# 1. Copy the environment file first to leverage Docker caching
COPY environment.yml /app/environment.yml

# 2. Install all system dependencies and the Conda environment at once
RUN apt update && \
    apt -y upgrade && \
    apt install -y nginx supervisor netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 3. Create the environment from your environment.yml
# This ensures Python, pip, virtualenv, and poetry match perfectly
RUN micromamba install -y -n base -f /app/environment.yml && \
    micromamba clean --all --yes

# Activate the environment globally for subsequent steps
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# 4. Turn off virtualenv creation so poetry installs directly into the conda env
RUN poetry config virtualenvs.create false

# 5. Copy project source files
COPY pyproject.toml poetry.lock README.md entry_point.py /app/
COPY data_access_service /app/data_access_service
COPY log_config.yaml /app/log_config.yaml

# 6. Install poetry dependencies
RUN poetry install --no-root

COPY das_site.conf  /etc/nginx/sites-available/
RUN ln -s /etc/nginx/sites-available/das_site.conf /etc/nginx/sites-enabled/ \
    && rm /etc/nginx/sites-enabled/default \
    && nginx -t

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
