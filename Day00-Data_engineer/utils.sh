#!/bin/bash

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
