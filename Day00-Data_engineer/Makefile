# ============================================================
#                           MAKEFILE
# ============================================================
# This Makefile is designed to automate every task in this day00 project:
# Variables are already properly set, so that you can just type
# Makefile's rules. Read more about them with `make help`.
# Remind that if you change a variable written in the .env,
# you must `rm .env ; make env`.


# ========================= VARIABLES ========================
# Makefile
MAKEFLAGS += --no-print-directory
.DEFAULT_GOAL := help

#  Files
ADMINER_SH := adminer.sh
OUTPUT_ZIP := subject.zip
URL := https://cdn.intra.42.fr/document/document/23499/subject.zip
#  Directories
MAKEFILE_DIR := makefile
SCRIPTS_DIR := scripts
SUBJECT_DIR := subject
#  Exercises
EX01_DIR := $(SCRIPTS_DIR)
EX01_PY := ex01.py
EX01_TABLE := example
EX03_DIR := ex03
EX03_PY := automatic_table.py
EX04_DIR := ex04
EX04_PY := items_table.py
LOGS_TABLE := logs

# Docker
#  Infos
DOCKER_NETWORK := network
ADMINER_PORT := 8080
ADMINER_SYSTEM := PostgreSQL
POSTGRES_DB := piscineds
POSTGRES_HOST := localhost
POSTGRES_PASSWORD := mysecretpassword
POSTGRES_PORT := 5432
POSTGRES_USER := nstoutze
PY_LOG_LEVEL := WARNING
#  Containers' name
ADMINER_CONTAINER := adminer
POSTGRES_CONTAINER := postgres
PYTHON_CONTAINER := python
#  Directories
DATA_VOLUME := data
PYTHON_APP_DIR := app
#  Files
DC_COMPOSE := docker-compose.yaml
DOWN_SH := down.sh
EX02_SQL := ex02/table.sql
EX02_TABLE := data_2022_nov
EX02_CSV_FILE := /$(DATA_VOLUME)/customer/$(EX02_TABLE).csv
EX02_LOGS_BEFORE_PY := ex02_logs_before.py
EX02_LOGS_AFTER_PY := ex02_logs_after.py
DROP_TABLE_PY := drop_table.py
REQUIREMENTS := requirements.txt
SETUP_LOG_TABLE := setup_log_table.py
#  Commands
DOCKER_EXEC := docker exec -it
RM_VOLUMES_SH := rm_volumes.sh
PSQL_OPTIONS := -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST) -W
PSQL := $(DOCKER_EXEC) $(POSTGRES_CONTAINER) psql $(PSQL_OPTIONS)
DOCKER_PYTHON := $(DOCKER_EXEC) $(PYTHON_CONTAINER) python3


# ========================= RULES =========================

.PHONY: adminer docker_checks down download drop end env \
		ex00 ex01 ex02 ex03 ex04 \
		fclean help postgres_container psql python_container \
		psql_without_password rebuild_image restart_containers_env_changed \
		rm_volumes sqli unzip up wait_for_postgres \


adminer:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(ADMINER_SH)


docker_checks:
	@printf "\n"
	docker ps
	@printf "\n"
	docker volume ls
	@printf "\n"


down:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(DOWN_SH)
	$(MAKE) docker_checks


download:
	@if [ -f $(OUTPUT_ZIP) ]; then \
		echo "$(OUTPUT_ZIP) already exists. Skipping download."; \
	else \
		echo "Downloading $(OUTPUT_ZIP) from $(URL)..."; \
		curl -o $(OUTPUT_ZIP) -L $(URL); \
		echo "Download complete: $(OUTPUT_ZIP)"; \
	fi
#	-L makes curl follow potential (and frequent) URL chains, in order to reach the targeted file.


drop:
	@if [ -z "$(TABLE)" ]; then \
		printf "ERROR: No table name provided !\nUsage: make drop TABLE=<table_name>\n"; \
		exit 1; \
	fi
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(DROP_TABLE_PY) $(TABLE)


end: down fclean


env:
	@if [ -f .env ]; then \
		echo ".env file already exists. Skipping creation."; \
	else \
		echo "Creating .env file..."; \
		echo "# Makefile" >> .env; \
		echo "#" >> .env; \
		echo "#  Directories" >> .env; \
		echo "SUBJECT_DIR=$(SUBJECT_DIR)" >> .env; \
		echo "SCRIPTS_DIR=$(SCRIPTS_DIR)" >> .env; \
		echo "EX03_DIR=$(EX03_DIR)" >> .env; \
		echo "EX04_DIR=$(EX04_DIR)" >> .env; \
		echo "#" >> .env; \
		echo "#" >> .env; \
		echo "# Docker" >> .env; \
		echo "#" >> .env; \
		echo "#  Containers' infos" >> .env; \
		echo "ADMINER_CONTAINER=$(ADMINER_CONTAINER)" >> .env; \
		echo "ADMINER_PORT=$(ADMINER_PORT)" >> .env; \
		echo "ADMINER_SYSTEM=$(ADMINER_SYSTEM)" >> .env; \
		echo "DATA_VOLUME=$(DATA_VOLUME)" >> .env; \
		echo "DOCKER_NETWORK=$(DOCKER_NETWORK)" >> .env; \
		echo "LOGS_TABLE=$(LOGS_TABLE)" >> .env; \
		echo "POSTGRES_CONTAINER=$(POSTGRES_CONTAINER)" >> .env; \
		echo "PY_LOG_LEVEL=$(PY_LOG_LEVEL)" >> .env; \
		echo "POSTGRES_DB=$(POSTGRES_DB)" >> .env; \
		echo "POSTGRES_HOST=$(POSTGRES_HOST)" >> .env; \
		echo "POSTGRES_PASSWORD=$(POSTGRES_PASSWORD)" >> .env; \
		echo "POSTGRES_PORT=$(POSTGRES_PORT)" >> .env; \
		echo "POSTGRES_USER=$(POSTGRES_USER)" >> .env; \
		echo "PYTHON_CONTAINER=$(PYTHON_CONTAINER)" >> .env; \
		echo "PYTHON_DOCKERFILE=Dockerfile_python" >> .env; \
		echo "#" >> .env; \
		echo "#  Files" >> .env; \
		echo "EX01_PY=$(EX01_PY)" >> .env; \
		echo "EX01_TABLE=$(EX01_TABLE)" >> .env; \
		echo "EX02_SQL=$(EX02_SQL)" >> .env; \
		echo "EX02_CSV_FILE=$(EX02_CSV_FILE)" >> .env; \
		echo "EX02_TABLE=$(EX02_TABLE)" >> .env; \
		echo "INIT_PG_HBA_SH=init_pg_hba.sh" >> .env; \
		echo "POSTGRES_DOCKERFILE=Dockerfile_postgres" >> .env; \
		echo "PG_HBA_CONF=pg_hba.conf" >> .env; \
		echo "REQUIREMENTS=$(REQUIREMENTS)" >> .env; \
		echo "#" >> .env; \
		echo "#  Directories" >> .env; \
		echo "PYTHON_APP_DIR=$(PYTHON_APP_DIR)" >> .env; \
		echo "#" >> .env; \
		echo ".env file created successfully."; \
	fi


ex00: up psql
	@printf "\nYou may want to test a connection without the -W option:\n \
	make psql_without_password\n"


ex01: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX01_DIR)/$(EX01_PY)
	@$(MAKE) adminer


ex02: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(EX02_LOGS_BEFORE_PY)

	@read -p "Proceed anyway with creation and import of $(EX02_TABLE) ? (yes/no): " user_input; \
		if [ "$$user_input" = "yes" ] || [ "$$user_input" = "y" ] || [ "$$user_input" = "" ]; then \
			echo "$(EX02_TABLE) creation and import..."; \
			if [ -f $(EX02_SQL) ]; then \
				echo "Executing SQL file with dynamic path..."; \
				$(PSQL) -v file="$(EX02_CSV_FILE)" -v table="$(EX02_TABLE)" -f $(EX02_SQL); \
				echo "Table created and data imported successfully!"; \
			else \
				echo "Error: $(EX02_SQL) not found."; \
			fi; \
			$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(EX02_LOGS_AFTER_PY); \
		else \
			echo "Import aborted."; \
		fi
#		-v is a psql option for sending a variable
#		-f is a psql option for running .sql files


ex03: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX03_DIR)/$(EX03_PY)


ex04: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX04_DIR)/$(EX04_PY)


fclean:
	rm -rf $(SUBJECT_DIR) $(OUTPUT_ZIP) $(APP_DIR_NAME) .env || true
	$(MAKE) rm_volumes


help:
	@bash $(SCRIPTS_DIR)/$(MAKEFILE_DIR)/help.sh $(RULE)


postgres_container:
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER) bash


psql: up wait_for_postgres
	$(PSQL)


psql_without_password:
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST)


python_container:
	$(DOCKER_EXEC) $(PYTHON_CONTAINER) bash


rebuild_image:
	@if [ -z "$(SERVICE)" ]; then \
		printf "ERROR: No service name provided!\n \
		Usage: make rebuild_image SERVICE=<service_name>\n"; \
		exit 1; \
	fi

	@if [ "$(SERVICE)" = "$(PYTHON_CONTAINER)" ]; then \
		docker-compose kill $(PYTHON_CONTAINER); \
	else \
		docker-compose stop $(SERVICE); \
	fi

	docker-compose build $(SERVICE)
	docker-compose up -d $(SERVICE)
	printf "Service $(SERVICE) rebuilt and started successfully.\n"


restart_containers_env_changed:
	$(MAKE) down
	rm .env
	$(MAKE) env
	$(MAKE) up


rm_volumes:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(RM_VOLUMES_SH)


sqli: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/vulnerable_sql.py


unzip: download
	@if [ -d $(SUBJECT_DIR) ]; then \
		echo "Directory $(SUBJECT_DIR) already exists. Skipping extraction."; \
	elif [ -f $(OUTPUT_ZIP) ]; then \
		echo "Extracting $(OUTPUT_ZIP)..."; \
		unzip -o $(OUTPUT_ZIP); \
		echo "Extraction complete: files are in $(SUBJECT_DIR)/"; \
	else \
		echo "$(OUTPUT_ZIP) not found. Please run 'make download' first."; \
	fi


up: env unzip
	docker-compose -f $(DC_COMPOSE) up -d
	$(MAKE) docker_checks
	$(MAKE) wait_for_postgres
	$(DOCKER_PYTHON) $(SCRIPTS_DIR)/$(SETUP_LOG_TABLE)

	@printf "\nReminder: if a Dockerfile has been modified, \
	you must stop, rebuild and up it.\n \
	make rebuild_image SERVICE=<service_name>\n\n"; \


wait_for_postgres:
	@echo "Checking if PostgreSQL is ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(POSTGRES_CONTAINER) pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST) && break || sleep 2; \
	done
	@printf "\nPostgreSQL is ready!\n\n"
