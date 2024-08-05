#!/bin/bash

docker build --no-cache -t data-access-service . && docker run -p 8000:8000 data-access-service
