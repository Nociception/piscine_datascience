# TODO LIST

# analyze and vacuum item, or item_deduplicated
# makefile rule clean every tests ex03


# refactor the decorator
# secure the password freely, as this day01 does not talk about it !!
# remind to write sqli-proof queries
# add the subject for each exercise
# rewrite help.sh file
# DOCSTRING
# flake8
# update .PHONY
# Add a logger.info message into the QueryInfo objects for better logs message launching
# Add a logfile to store every logs




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
FEB_CSV = data_2023_feb.csv
OUTPUT_ZIP := subject.zip
URL := https://cdn.intra.42.fr/document/document/23499/subject.zip
URL_FEB_CSV := https://cdn.intra.42.fr/document/document/28095/data_2023_feb.csv
#  Directories
MAKEFILE_DIR := makefile
SCRIPTS_DIR := scripts
SUBJECT_DIR := subject
#  Exercises
EX00_DIR := $(SCRIPTS_DIR)
EX00_PY := ex00.py
EX00_TABLE := example
EX01_DIR := ex01
EX01_PY := customers_table.py
EX01_TABLE := customers
EX02_DIR := ex02
EX02_PY := remove_duplicates.py
EX02_TESTS_PY := ex02_tests.py
EX03_DIR := ex03
EX03_PY := fusion.py
EX03_TABLE := item
EX03_TESTS := ex03_tests.py
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
PY_LOG_LEVEL := INFO
#  Containers' name
ADMINER_CONTAINER := adminer
POSTGRES_CONTAINER := postgres
PYTHON_CONTAINER := python
#  Directories
DATA_VOLUME := data
PYTHON_APP_DIR := app
#  Files
ANALYZE_TABLE_PY := analyze_table.py
DC_COMPOSE := docker-compose.yaml
DOWN_SH := down.sh
DROP_TABLE_PY := drop_table.py
REQUIREMENTS := requirements.txt
SETUP_LOG_TABLE := setup_log_table.py
VACUUM_TABLE_PY := vacuum_table.py
#  Commands
DOCKER_EXEC := docker exec -it
RM_VOLUMES_SH := rm_volumes.sh
PSQL_OPTIONS := -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST) -W
PSQL := $(DOCKER_EXEC) $(POSTGRES_CONTAINER) psql $(PSQL_OPTIONS)
DOCKER_PYTHON := $(DOCKER_EXEC) $(PYTHON_CONTAINER) python3


# suivi : up

.PHONY: up
# TODO update

adminer:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(ADMINER_SH)


analyze: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(ANALYZE_TABLE_PY) $(TABLE)
	

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

	@if [ -f $(FEB_CSV) ] || [ -f $(SUBJECT_DIR)/customer/$(FEB_CSV) ]; then \
		echo "$(FEB_CSV) already exists. Skipping download."; \
	else \
		echo "Downloading $(FEB_CSV) from $(URL_FEB_CSV)..."; \
		curl -o $(SUBJECT_DIR)/customer/$(FEB_CSV) -L $(URL_FEB_CSV); \
		echo "Download complete: $(FEB_CSV)"; \
	fi


drop:
	@if [ -z "$(TABLE)" ]; then \
		printf "ERROR: No table name provided !\nUsage: make drop TABLE=<table_name>\n"; \
		exit 1; \
	fi
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(DROP_TABLE_PY) $(TABLE)


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
		echo "EX01_DIR=$(EX01_DIR)" >> .env; \
		echo "EX02_DIR=$(EX02_DIR)" >> .env; \
		echo "EX03_DIR=$(EX03_DIR)" >> .env; \
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
		echo "EX00_PY=$(EX00_PY)" >> .env; \
		echo "EX00_TABLE=$(EX00_TABLE)" >> .env; \
		echo "EX01_PY=$(EX01_PY)" >> .env; \
		echo "EX01_TABLE=$(EX01_TABLE)" >> .env; \
		echo "EX02_PY=$(EX02_PY)" >> .env; \
		echo "EX02_TABLE=$(EX02_TABLE)" >> .env; \
		echo "EX03_DIR=$(EX03_PY)" >> .env; \
		echo "EX03_TABLE=$(EX03_TABLE)" >> .env; \
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


ex00: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX00_DIR)/$(EX00_PY)
	@$(MAKE) adminer


ex01: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX01_DIR)/$(EX01_PY)


ex02: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX02_DIR)/$(EX02_PY)
	@printf "\nYou might want to run 'make ex02-demo_test' to see \
	how the SQL query to remove duplicates affects a table, \
	on a 60 rows one, which contains many duplicates cases.\n"


ex02-demo_test: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(EX02_TESTS_PY)


ex03: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX03_DIR)/$(EX03_PY)


ex03-tests: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(EX03_TESTS)


end: down fclean


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
	printf "Service $(SERVICE) rebuilt and restarted successfully.\n"


restart_containers_env_changed:
	$(MAKE) down
	rm .env
	$(MAKE) env
	$(MAKE) up


rm_volumes:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(RM_VOLUMES_SH)


sqli: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/vulnerable_sql.py


up: env unzip
	docker-compose -f $(DC_COMPOSE) up -d
	$(MAKE) docker_checks
	$(MAKE) wait_for_postgres
	$(DOCKER_PYTHON) $(SCRIPTS_DIR)/$(SETUP_LOG_TABLE)

	@printf "\nReminder: if a Dockerfile has been modified, \
	you must stop, rebuild and up it.\n \
	make rebuild_image SERVICE=<service_name>\n\n"; \


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


vacuum: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(VACUUM_TABLE_PY) $(TABLE)


wait_for_postgres:
	@echo "Checking if PostgreSQL is ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(POSTGRES_CONTAINER) pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST) && break || sleep 2; \
	done
	@printf "\nPostgreSQL is ready!\n\n"


# J'ai donc ma table item_deduplicated de prete.

# La derniere etape consiste donc l'inclure dans customers.
# Rappel, customers ressemble a ceci :
#        event_time       |    event_type    | product_id | price |  user_id  |             user_session             
# ------------------------+------------------+------------+-------+-----------+--------------------------------------
#  2022-10-01 00:00:00+00 | cart             |    5773203 |  2.62 | 463240011 | 26dd6e6e-4dac-4778-8d2c-92e149dab885
#  2022-10-01 00:00:03+00 | cart             |    5773353 |  2.62 | 463240011 | 26dd6e6e-4dac-4778-8d2c-92e149dab885
#  2022-10-01 00:00:07+00 | cart             |    5881589 | 13.48 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:07+00 | cart             |    5723490 |  2.62 | 463240011 | 26dd6e6e-4dac-4778-8d2c-92e149dab885
#  2022-10-01 00:00:15+00 | cart             |    5881449 |  0.56 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:16+00 | cart             |    5857269 |  2.62 | 430174032 | 73dea1e7-664e-43f4-8b30-d32b9d5af04f
#  2022-10-01 00:00:19+00 | cart             |    5739055 |  4.75 | 377667011 | 81326ac6-daa4-4f0a-b488-fd0956a78733
#  2022-10-01 00:00:24+00 | cart             |    5825598 |  0.56 | 467916806 | 2f5b5546-b8cb-9ee7-7ecd-84276f8ef486
#  2022-10-01 00:00:25+00 | cart             |    5698989 |  1.27 | 385985999 | d30965e8-1101-44ab-b45d-cc1bb9fae694
#  2022-10-01 00:00:26+00 | view             |    5875317 |  1.59 | 474232307 | 445f2b74-5e4c-427e-b7fa-6e0a28b156fe
#  2022-10-01 00:00:28+00 | view             |    5692917 |  5.54 | 555446068 | 4257671a-efc8-4e58-96c2-3ab457916d78
#  2022-10-01 00:00:28+00 | remove_from_cart |    5834172 |  0.95 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:30+00 | remove_from_cart |    5809103 |  0.60 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:30+00 | remove_from_cart |    5809103 |  0.60 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:32+00 | remove_from_cart |    5779403 | 12.22 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:33+00 | remove_from_cart |    5779403 | 12.22 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:34+00 | cart             |    5670337 |  2.38 | 546705258 | 3b5c65c0-bb1c-453b-b340-4ebf973a3136
#  2022-10-01 00:00:42+00 | cart             |    5836522 |  0.40 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:43+00 | cart             |    5836522 |  0.40 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:00:48+00 | view             |    5819638 | 21.75 | 546705258 | 3b5c65c0-bb1c-453b-b340-4ebf973a3136
#  2022-10-01 00:00:48+00 | cart             |    5859414 |  2.37 | 555442940 | 618f3d7d-2939-47ea-8f1d-07a4f97d0fe2
#  2022-10-01 00:00:53+00 | view             |    5856191 | 24.44 | 507355498 | 944c7e9b-40bd-4112-a05b-81e73f37e0c0
#  2022-10-01 00:00:55+00 | cart             |    5859413 |  2.37 | 555442940 | 618f3d7d-2939-47ea-8f1d-07a4f97d0fe2
#  2022-10-01 00:00:56+00 | remove_from_cart |    5881589 | 13.48 | 429681830 | 49e8d843-adf3-428b-a2c3-fe8bc6a307c9
#  2022-10-01 00:01:01+00 | cart             |    5723518 |  2.62 | 430174032 | c2bbd970-a5ad-42dd-a59b-f44276330b02
# (25 rows)


# Et item_deduplicated ressemble a ceci:
# piscineds=# SELECT * FROM item_deduplicated LIMIT 25;
#  product_id |     category_id     | category_code |   brand   
# ------------+---------------------+---------------+-----------
#     5823501 | 1487580004916134656 |               | 
#     5838646 | 1487580011283087360 |               | pnb
#     5870298 | 1487580013950664960 |               | estel
#     5857192 | 1487580005880824576 |               | irisk
#       72397 | 1487580011585077248 |               | skinlite
#     5864081 | 1487580013950664960 |               | deoproce
#     5878981 | 1487580013254410496 |               | 
#     5828048 | 1487580009286598656 |               | solomeya
#     5906112 | 1487580007675986944 |               | bpw.style
#        3936 | 1487580005343953664 |               | entity
#     5867117 | 1487580008758116352 |               | domix
#     5900634 | 1487580007675986944 |               | freedecor
#     5803508 | 1487580011652186112 |               | 
#     5798928 | 1783999068909863680 |               | zinger
#     5875286 | 1487580009605365760 |               | 
#     5808018 | 2151191070984110848 |               | irisk
#     5557788 | 1487580005528503040 |               | pnb
#     5563776 | 1487580005595611904 |               | 
#     5885387 | 1487580007675986944 |               | bpw.style
#       33957 | 1487580005411062528 |               | cnd
#     5823601 | 1487580004916134656 |               | 
#     5887002 | 1487580006317032448 |               | 
#     5895795 | 1487580012524601600 |               | 
#     5898387 | 1783999072332415232 |               | ecolab
#     5770046 | 1487580013841613056 |               | kapous
# (25 rows)

# (a noter que la premiere colonne est toujours renseignee, quand pour les trois autres, elles peuvent contenir quelque chose ou pas)

# L'idee est donc de:
# - ajouter trois colonnes a customers (on va faire un ALTER quelque chose je pense)

# (on sait que chaque ligne de item_deduplicated est maintenant unique du point de vue de sa colonne product_id)
# - puis faire en sorte que: pour chaque ligne de customers, les nouvelles colonnes soit remplies avec celles presentes dans item_deduplicated selon la ligne du product_id correspondant ; me suis-je bien fait comprendre ? Dans le doute, reformule ma requete, et illustre avec un exemple suffisamment complet.