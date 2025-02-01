#!/bin/bash

source "$(dirname "$0")/utils.sh"


restart_python_container_if_needed() {
    args_number_exact_check $# 1 || return 1

    local diff_detected=$1

    if [ "$diff_detected" -eq 1 ]; then
        printf "\nRestarting Python container...\n"
        docker kill python
        docker-compose up -d python
        printf "Python container restarted.\n\n"
    else
        printf "No differences found. No updates nor container restart needed.\n\n"
    fi
}


update_py_files() {
    local diff_detected=0
    python_files=()

    mapfile -d '' python_files < <(find ex*/ -type f -name "*.py" -print0)

    if [ ${#python_files[@]} -eq 0 ]; then
        printf "\n\nNo Python files found in ex*/ directories.\n"
        return 0
    fi

    printf "\n\nChecking for differences in Python files...\n"

    for source_file in "${python_files[@]}"; do
        dest_file="app/$(basename "$source_file")"

        if file_differs "$source_file" "$dest_file"; then
            printf "Processing: $source_file → Updating $dest_file\n"
            cp "$source_file" "$dest_file" && echo "Copied: $source_file → $dest_file"
            diff_detected=1
        fi
    done

    restart_python_container_if_needed "$diff_detected"
}

update_py_files
