#!/bin/bash

set -o allexport
source "$(dirname "$0")/../.env" 2>/dev/null || echo "Warning: .env file not found"
set +o allexport

args_number_exact_check() {
    local received=$1
    local expected=$2
    if [ "$received" -eq "$expected" ]; then
        return 0
    else
        printf "Error: %d args received, instead of %d expected.\n" "$received" "$expected"
        return 1
    fi
}


file_differs() {
    printf "\nChecking if files '$1' and '$2' are different...\n"
    
    args_number_exact_check $# 2 || return 1

    local file1="$1"
    local file2="$2"

    if ! diff "$file1" "$file2" > /dev/null 2>&1; then
        printf "Files '$file1' and '$file2' are different.\n\n"
        return 0
    else
        printf "Files '$file1' and '$file2' are identical.\n\n"
        return 1
    fi
}


copy_file() {
    local source_file="$1"
    local destination_file="$2"

    printf "Processing: $source_file → Updating $destination_file\n"

    if cp "$source_file" "$destination_file"; then
        printf "Successfully copied: $source_file → $destination_file\n"
        return 0
    else
        printf "Error: Fail to copy: $source_file → $destination_file\n"
        return 1
    fi
}


make_directory() {
    local dir="$1"

    if [ -d "$dir" ]; then
        printf "\nDirectory '%s' already exists. Skipping creation.\n\n" "$dir"
    else
        mkdir "$dir"
        printf "\nDirectory '%s' created.\n\n" "$dir"
    fi
}


copy_update_file_if_needed() {
    local source="$1"
    local destination="$2"

    args_number_exact_check $# 2 || return 1

    if [ ! -f "$destination" ] || file_differs "$source" "$destination"; then
        printf "\nCopying %s → %s\n" "$source" "$destination\n"
        cp "$source" "$destination"
        printf "Copied: %s → %s\n" "$source" "$destination\n\n"
        return 1
    else
        printf "Skipping copy: %s (already up to date)\n" "$destination\n\n"
        return 0
    fi
}


retrieve_all_py_files() {
    args_number_exact_check $# 1 || return 1

    local -n result_array="$1"
    result_array=()

    mapfile -d '' result_array < <(find ex*/ scripts/ -type f -name "*.py" -print0)

    if [ ${#result_array[@]} -eq 0 ]; then
        printf "\n\nNo Python files found in ex*/ directories.\n"
    else
        printf "\n\nFound %d Python files.\n" "${#result_array[@]}"
    fi
}


sync_python_files_to_app() {
    local diff_detected=0
    local python_files=()

    retrieve_all_py_files python_files

    if [ ${#python_files[@]} -eq 0 ]; then
        printf "\nNo Python files found in ex*/ directories.\n"
        return 0
    fi

    printf "\nSyncing Python files to '%s'...\n" "$APP_DIR"

    for source_file in "${python_files[@]}"; do
        dest_file="$APP_DIR/$(basename "$source_file")"

        if file_differs "$source_file" "$dest_file"; then
            copy_file "$source_file" "$dest_file"
            diff_detected=1
        fi
    done

    return $diff_detected
}
