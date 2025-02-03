#!/bin/bash

set -o allexport
source "$(dirname "$0")/../.env" 2>/dev/null || echo "Warning: .env file not found"
set +o allexport

printf "Stopping and removing Docker containers..."

docker kill "$PYTHON_CONTAINER" 2>/dev/null|| true

docker-compose -f docker-compose.yaml down

if [ -n "$(docker ps -aq)" ]; then
    docker rm -f $(docker ps -aq)
else
    printf "No containers to remove.\n"
fi