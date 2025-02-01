#!/bin/bash

set -o allexport
source .env 2>/dev/null || echo "Warning: .env file not found"
set +o allexport

source "$(dirname "$0")/utils.sh"


make_directory "$APP_DIR"


sync_python_files_to_app


if [ -f "$REQ_FILE" ]; then
    dest_req="$APP_DIR/$REQ_FILE"
    copy_update_file_if_needed "$REQ_FILE" "$dest_req"
else
    printf "'%s' not found, skipping...\n" "$REQ_FILE"
fi

printf "Directory '%s' is now properly set up.\n\n" "$APP_DIR"
