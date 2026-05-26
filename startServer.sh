#!/bin/bash

if [[ "$1" == "--docker" ]]; then
    echo "Starting in Docker mode..."
    docker compose down && docker compose build && docker compose up
else
    # Simulate server side limited memory environment
    echo "Starting locally with 3G memory limit via systemd-run..."
    systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0 python -m data_access_service.server
fi
