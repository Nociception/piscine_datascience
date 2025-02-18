#!/bin/bash

case "$1" in
    "adminer")
        echo "adminer: Display instruction to reach Adminer interface in a web browser."
        ;;
    "docker_checks")
        echo "docker_checks: Displays running Docker containers and associated volumes."
        ;;
    "down")
        echo "down: Stops and deletes the Docker containers."
        ;;
    "download")
        echo "download: Downloads the subject.zip file if it does not already exist."
        ;;
    "drop")
        echo "drop TABLE=<table_name>: Deletes a table in PostgreSQL."
        ;;
    "end")
        echo "end: Executes 'down' followed by 'fclean'."
        ;;
    "env")
        echo "env: Creates the .env file if it does not already exist."
        ;;
    "ex00")
        echo "ex00: Starts the containers and opens psql."
        ;;
    "ex01")
        echo "ex01: Runs the Python script ex01.py inside the Python container."
        ;;
    "ex02")
        echo "ex02: Executes scripts before and after creating the table in PostgreSQL."
        ;;
    "ex03")
        echo "ex03: Runs the Python script automatic_table.py."
        ;;
    "ex04")
        echo "ex04: Runs the Python script items_table.py."
        ;;
    "fclean")
        echo "fclean: Removes temporary files and Docker volumes."
        ;;
    "postgres_container")
        echo "postgres_container: Opens a shell inside the PostgreSQL container."
        ;;
    "psql")
        echo "psql: Opens a psql session inside the PostgreSQL container."
        ;;
    "python_container")
        echo "python_container: Opens a shell inside the Python container."
        ;;
    "rebuild_image")
        echo "rebuild_image SERVICE=<service>: Rebuilds a Docker image and restarts the service."
        ;;
    "restart_containers_env_changed")
        echo "restart_containers_env_changed: downs the containers, deletes and recreates .env, and then ups back the containers."
        ;;
    "rm_volumes")
        echo "rm_volumes: Deletes Docker volumes."
        ;;
    "sqli")
        echo "sqli: Runs the Python script vulnerable_sql.py."
        ;;
    "unzip")
        echo "unzip: Extracts the subject.zip file."
        ;;
    "up")
        echo "up: Starts the services using docker-compose."
        ;;
    "wait_for_postgres")
        echo "wait_for_postgres: Checks if PostgreSQL is ready."
        ;;
    "")
        echo "Usage: make help RULE=<rule>"
        ;;
    *)
        echo "Unknown rule: '$1'"
        ;;
esac
