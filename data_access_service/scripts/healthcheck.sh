#!/bin/sh

# Run the health_check function
poetry run python -c "from data_access_service.core.restapi import health_check; print(health_check().get_data(as_text=True))"