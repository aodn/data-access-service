#!/bin/bash

docker build --progress=plain --no-cache -t data-access-service . && docker run -p 8000:8000 data-access-service