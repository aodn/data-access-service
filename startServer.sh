#!/bin/bash

# Default values
DOCKER_MODE=false
RELOAD_MODE=false

# Parse arguments
for arg in "$@"; do
    if [[ "$arg" == "--docker" ]]; then
        DOCKER_MODE=true
    elif [[ "$arg" == "--reload" ]]; then
        RELOAD_MODE=true
    fi
done

if [ "$DOCKER_MODE" = true ]; then
    echo "Starting in Docker mode..."
    docker compose down && docker compose build && docker compose up
else
    echo "Starting locally with 3G memory limit via systemd-run..."
    if [ "$RELOAD_MODE" = true ]; then
        echo "Reload mode enabled (auto-reloading on code changes)."
        export FASTAPI_RELOAD=true
    else
        echo "Reload mode disabled (single process, optimized memory footprint)."
        export FASTAPI_RELOAD=false
    fi
    systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0 python -m data_access_service.server
fi
