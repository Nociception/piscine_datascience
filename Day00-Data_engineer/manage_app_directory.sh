#!/bin/bash

set -o allexport
source .env 2>/dev/null || echo "Warning: .env file not found"
set +o allexport

source "$(dirname "$0")/utils.sh"

make_directory "$APP_DIR"

printf "\nSearching for Python files in ex*/ directories...\n"
mapfile -d '' python_files < <(find ex*/ -type f -name "*.py" -print0)

if [ ${#python_files[@]} -eq 0 ]; then
    printf "No Python files found in ex*/ directories.\n"
else
    for file in "${python_files[@]}"; do
        dest_file="$APP_DIR/$(basename "$file")"
        copy_update_file_if_needed "$file" "$dest_file"
    done
fi

if [ -f "$REQ_FILE" ]; then
    dest_req="$APP_DIR/$REQ_FILE"
    copy_update_file_if_needed "$REQ_FILE" "$dest_req"
else
    printf "'%s' not found, skipping...\n" "$REQ_FILE"
fi

printf "Directory '%s' is now properly set up.\n\n" "$APP_DIR"
