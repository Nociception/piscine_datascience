#!/bin/bash

printf "\nThis will permanently remove all Docker volumes, \
which contain (among other things) data tables.\n"
read -p "Are you sure you want to continue? (y/N): " confirm
printf "\n"

if [[ "$confirm" =~ ^[Yy]$ ]]; then
    if [ -n "$(docker volume ls -q)" ]; then
        printf "Removing all Docker volumes...\n"
        docker volume rm $(docker volume ls -q)
        printf "All volumes have been removed.\n"
    else
        printf "No volumes found to remove.\n"
    fi
else
    printf "Operation cancelled.\n"
fi
