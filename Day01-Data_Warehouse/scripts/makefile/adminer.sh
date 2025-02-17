#!/bin/bash

set -o allexport
source "$(dirname "$0")/../../.env" 2>/dev/null || echo "Warning: .env file not found"
set +o allexport

printf "\nTo use $ADMINER_CONTAINER, follow these steps:
1. Open your browser and go to: http://localhost:$ADMINER_PORT
2. Fill in the following connection details:
   - System: $ADMINER_SYSTEM
   - Server: $POSTGRES_CONTAINER
   - Username: $POSTGRES_USER
   - Password: $POSTGRES_PASSWORD
   - Database: $POSTGRES_DB\n"
