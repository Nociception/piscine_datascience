#TODO
# checklogtable.py must be reforged
# Path().name in the logs_table_filler, instead of writing it when parameters are sent to
#	logs_table_filler
# write the logs_checker step before running the ex02/table.sql file
#	to show what are the last table manipulation in the logs table
# logs_table_filler: factor it with new function table_exists
# show the logs table in case of some lines are already in the targeted table
# ex01 and 02 are now somehow broken ; must fix them, with the new logs table process
# check every commands in the day00 ; also complete the .PHONY
# check how to import properly a class (if from Class import Class is needed, or if it can be shorter/smarter)
# write a makefile rule (and a related .py file) to drop a table and log it

# study how python import can be shorter/smarter
# clean commented lines in the code
# check useless/ghost makefile variable
# write a docstring for this makefile
# write the help rule
# color log messages
# flake8
# secure data volume, in case of a deletion attempt
# check again every python function DOCSTRING
# check invalid option -W (it should be there, but it currently leads to an error)

# Makefile

#  Files
URL = https://cdn.intra.42.fr/document/document/23499/subject.zip
OUTPUT_ZIP = subject.zip
ADMINER_SH = adminer.sh
#  Directories
SUBJECT_DIR = subject
SCRIPTS_DIR = scripts
MAKEFILE_DIR = makefile
#  Exercices
LOGS_TABLE = logs
EXAMPLE_TABLE = example_table
EX01_SQL = ex01.sql
EX01_SQL_HOST = $(SCRIPTS_DIR)/$(EX01_SQL)
EX01_SQL_CONTAINER = ex01/$(EX01_SQL)
EX03_DIR = ex03
EX03_PY = automatic_table.py
EX04_DIR = ex04
EX04_PY = items_table.py

# Docker
#  Infos
POSTGRES_PASSWORD = mysecretpassword
POSTGRES_USER = nstoutze
POSTGRES_DB = piscineds
POSTGRES_HOST = localhost
POSTGRES_PORT = 5432
ADMINER_SYSTEM = PostgreSQL
ADMINER_PORT = 8080
DOCKER_NETWORK = network
#  Containers' name
POSTGRES_CONTAINER = postgres
PYTHON_CONTAINER = python
ADMINER_CONTAINER = adminer
#  Directories
PYTHON_APP_DIR = app
DATA_VOLUME = data
#  Files
DC_COMPOSE = docker-compose.yaml
DOWN_SH = down.sh
REQUIREMENTS = requirements.txt
SETUP_LOG_TABLE = setup_log_table.py
CHECK_LOG_TABLE = check_log_table.py
EX02_SQL = ex02/table.sql
EX02_TABLE = data_2022_dec
EX02_CSV_FILE = /$(DATA_VOLUME)/customer/$(EX02_TABLE).csv
EX02_LOGS_PY = ex02_logs.py
#  Commands
DOCKER_EXEC = docker exec -it
RM_VOLUMES_SH = rm_volumes.sh
PSQL_OPTIONS = -U $(POSTGRES_USER) -d $(POSTGRES_DB) -h $(POSTGRES_HOST)
PSQL = $(DOCKER_EXEC) $(POSTGRES_CONTAINER) psql $(PSQL_OPTIONS)
DOCKER_PYTHON = $(DOCKER_EXEC) $(PYTHON_CONTAINER) python3


.PHONY: up psql down rm_volumes env end fclean \
		docker_checks postgres_container python_container rebuild_image\
		ex00 \
		ex01-setup ex01-clean \
		download unzip ex02 \
		ex03 \
		ex04 \


up: env unzip
	docker-compose -f $(DC_COMPOSE) up -d
	$(MAKE) docker_checks
	$(MAKE) wait_for_postgres
	$(DOCKER_PYTHON) $(SCRIPTS_DIR)/$(SETUP_LOG_TABLE)

	@printf "\nReminder: if a Dockerfile has been modified, \
	you must stop, rebuild and up it.\n \
	make rebuild_image SERVICE=<service_name>\n\n"; \


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
		echo "POSTGRES_DB=$(POSTGRES_DB)" >> .env; \
		echo "POSTGRES_HOST=$(POSTGRES_HOST)" >> .env; \
		echo "POSTGRES_PASSWORD=$(POSTGRES_PASSWORD)" >> .env; \
		echo "POSTGRES_PORT=$(POSTGRES_PORT)" >> .env; \
		echo "POSTGRES_USER=$(POSTGRES_USER)" >> .env; \
		echo "PYTHON_CONTAINER=$(PYTHON_CONTAINER)" >> .env; \
		echo "PYTHON_DOCKERFILE=Dockerfile_python" >> .env; \
		echo "#" >> .env; \
		echo "#  Files" >> .env; \
		echo "EX01_SQL_HOST=$(EX01_SQL_HOST)" >> .env; \
		echo "EX01_SQL_CONTAINER=$(EX01_SQL_CONTAINER)" >> .env; \
		echo "EX01_SQL=$(EX01_SQL)" >> .env; \
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


docker_checks:
	@printf "\n"
	docker ps
	@printf "\n"
	docker volume ls
	@printf "\n"


psql: wait_for_postgres
	$(PSQL)


postgres_container:
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER) bash


python_container:
	$(DOCKER_EXEC) $(PYTHON_CONTAINER) bash


wait_for_postgres:
	@echo "Checking if PostgreSQL is ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(POSTGRES_CONTAINER) pg_isready $(PSQL_OPTIONS) && break || sleep 2; \
	done
	@printf "\nPostgreSQL is ready!\n\n"
# pg_isready is postgresql tool which allows to connect to psql without authentication
# in order to retrieve availability information
# (useful to avoid to get error because of a too soon connection attempt).
# Return to the shell:
# - 0 : connection accepted
# - 1 : connection refused
# - 0 : no response from the psql server


down:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(DOWN_SH)
	$(MAKE) docker_checks


rm_volumes:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(RM_VOLUMES_SH)


download:
	@if [ -f $(OUTPUT_ZIP) ]; then \
		echo "$(OUTPUT_ZIP) already exists. Skipping download."; \
	else \
		echo "Downloading $(OUTPUT_ZIP) from $(URL)..."; \
		curl -o $(OUTPUT_ZIP) -L $(URL); \
		echo "Download complete: $(OUTPUT_ZIP)"; \
	fi
#	-L makes curl follow potential (and frequent) URL chains, in order to reach the targeted file.


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


fclean:
	rm -rf $(SUBJECT_DIR) $(OUTPUT_ZIP) $(APP_DIR_NAME) .env || true
	$(MAKE) rm_volumes


end: down fclean


adminer:
	./$(SCRIPTS_DIR)/$(MAKEFILE_DIR)/$(ADMINER_SH)


ex00: up psql


ex01-setup: up
	$(PSQL) -f /ex01/ex01.sql;


# 	@echo "Creating table $(EXAMPLE_TABLE)..."
# 	$(PSQL) -c "\
# 	CREATE TABLE IF NOT EXISTS $(EXAMPLE_TABLE) ( \
# 		event_time TIMESTAMP NOT NULL, \
# 		event_type VARCHAR(50), \
# 		product_id BIGINT, \
# 		category_id NUMERIC, \
# 		category_code VARCHAR(255), \
# 		brand VARCHAR(255), \
# 		price NUMERIC(10, 2), \
# 		user_id BIGINT, \
# 		user_session VARCHAR(255) \
# 	);"

# 	$(DOCKER_PYTHON) $(CHECK_LOG_TABLE) $(EXAMPLE_TABLE)

# 	@echo "Inserting data into $(EXAMPLE_TABLE) if empty..."
# 	$(PSQL) -c "\
# 	INSERT INTO $(EXAMPLE_TABLE) (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) \
# 	SELECT * FROM ( \
# 	    VALUES \
# 	    ('2022-10-01 00:00:00'::TIMESTAMP, 'cart', 5773203, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
# 	    ('2022-10-01 00:00:03'::TIMESTAMP, 'cart', 5773353, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
# 	    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5723490, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
# 	    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5881589, 215119107051219712, NULL, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
# 	    ('2022-10-01 00:00:15'::TIMESTAMP, 'cart', 5881449, 148758000513522845952, NULL, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
# 	    ('2022-10-01 00:00:16'::TIMESTAMP, 'cart', 5857269, 1487580005134238464, NULL, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f') \
# 	) AS new_data \
# 	WHERE NOT EXISTS (SELECT 1 FROM $(EXAMPLE_TABLE))";

# 	@echo "Logging insertion into table logs..."
# 	$(PSQL) -c "\
# 	BEGIN; \
# 	INSERT INTO logs (table_name, last_import) VALUES ('$(EXAMPLE_TABLE)', NOW()) \
# 	ON CONFLICT (table_name) DO UPDATE SET last_import = NOW(); \
# 	COMMIT;"

	@$(MAKE) adminer

ex01-clean:
	@echo "Dropping table $(EXAMPLE_TABLE)..."
	$(PSQL) -c "DROP TABLE IF EXISTS $(EXAMPLE_TABLE);"


ex02: up
	@if [ -f $(EX02_SQL) ]; then \
		echo "Executing SQL file with dynamic path..."; \
		$(PSQL) -v file="$(EX02_CSV_FILE)" -v table="$(EX02_TABLE)" -f $(EX02_SQL); \
		echo "Table created and data imported successfully!"; \
	else \
		echo "Error: $(EX02_SQL) not found."; \
	fi
#	-v is a psql option for sending a variable
#	-f is a psql option for running .sql files

test:
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(SCRIPTS_DIR)/$(EX02_LOGS_PY)



ex03: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX03_DIR)/$(EX03_PY)


ex04: up
	$(DOCKER_PYTHON) /$(PYTHON_APP_DIR)/$(EX04_DIR)/$(EX04_PY)


#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
