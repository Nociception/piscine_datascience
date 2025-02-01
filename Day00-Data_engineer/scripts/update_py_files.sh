#!/bin/bash

source "$(dirname "$0")/../scripts/utils.sh"


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


sync_python_files_to_app > /dev/null 2>&1
sync_result=$?
sync_result=$(echo "$sync_result" | tr -d '[:space:]')
restart_python_container_if_needed "$sync_result"
