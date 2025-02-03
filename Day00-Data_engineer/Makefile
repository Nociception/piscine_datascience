#TODO
# write a docstring for this makefile
# write the help rule
# color log messages
# protect the ex against double call makefile rule
# factor all python files
# check every commands in the day00
# write a sh file for the env, and ask the user to write his password
# write a sh file for adminer rule


DC_COMPOSE = docker-compose.yaml
DOWN_SH = down.sh
RM_VOLUMES_SH = rm_volumes.sh
DOCKER_EXEC = docker exec -it
POSTGRES_CONTAINER = postgres_piscineds
PSQL_OPTIONS = -U nstoutze -d piscineds -h localhost
PSQL = $(DOCKER_EXEC) $(POSTGRES_CONTAINER) psql $(PSQL_OPTIONS)
PYTHON_CONTAINER = python
ADMINER_CONTAINER = adminer
UPDATE_PY_FILES = update_py_files.sh
DOCKER_PYTHON = $(DOCKER_EXEC) $(PYTHON_CONTAINER) python3
EXAMPLE_TABLE = example_table
URL = https://cdn.intra.42.fr/document/document/23499/subject.zip
OUTPUT_ZIP = subject.zip
SUBJECT_DIR = subject
APP_DIR = app
REQ = requirements.txt
SCRIPTS_DIR = scripts
SETUP_LOG_TABLE = setup_log_table.py
CHECK_LOG_TABLE = check_log_table.py
LOGS_TABLE = logs
EX02_SQL = ex02/table.sql
EX03_PY = automatic_table.py
EX04_PY = items_table.py


.PHONY: up psql down rm_volumes env end fclean \
		docker_checks postgres_container python_container \
		ex00 \
		ex01-setup ex01-clean \
		download unzip ex02-import_data ex02-verify_data ex02-clean \
		ex03 \
		ex04 \


up: env unzip
	./$(SCRIPTS_DIR)/$(UPDATE_PY_FILES)
	docker-compose -f $(DC_COMPOSE) up -d
	$(MAKE) docker_checks
	$(MAKE) wait_for_postgres
	$(DOCKER_PYTHON) $(SETUP_LOG_TABLE)


env:
	@if [ -f .env ]; then \
		echo ".env file already exists. Skipping creation."; \
	else \
		echo "Creating .env file..."; \
		echo "POSTGRES_USER=nstoutze" > .env; \
		echo "POSTGRES_PASSWORD=mysecretpassword" >> .env; \
		echo "POSTGRES_DB=piscineds" >> .env; \
		echo "POSTGRES_HOST=localhost" >> .env; \
		echo "POSTGRES_PORT=5432" >> .env; \
		echo "POSTGRES_DOCKERFILE=Dockerfile_postgres" >> .env; \
		echo "PYTHON_DOCKERFILE=Dockerfile_python" >> .env; \
		echo "POSTGRES_CONTAINER=$(POSTGRES_CONTAINER)" >> .env; \
		echo "PYTHON_CONTAINER=$(PYTHON_CONTAINER)" >> .env; \
		echo "SUBJECT_DIR=$(SUBJECT_DIR)" >> .env; \
		echo "DATA_VOLUME=data" >> .env; \
		echo "APP_DIR=$(APP_DIR)" >> .env; \
		echo "REQ=$(REQ)" >> .env; \
		echo "LOGS_TABLE=$(LOGS_TABLE)" >> .env; \
		echo "ADMINER_CONTAINER=$(ADMINER_CONTAINER)" >> .env; \
		echo "ADMINER_PORT=8080" >> .env; \
		echo "DC_NETWORK=network" >> .env; \
		echo ".env file created successfully."; \
	fi
# Unable to make the makefile work with a heredoc.


docker_checks:
	docker ps
	docker volume ls


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
	@echo "PostgreSQL is ready!"
# pg_isready is postgresql tool which allows to connect to psql without authentication
# in order to retrieve availability information
# (useful to avoid to get error because of a too soon connection attempt).
# Return to the shell:
# - 0 : connection accepted
# - 1 : connection refused
# - 0 : no response from the psql server


down:
	./$(SCRIPTS_DIR)/$(DOWN_SH)
	$(MAKE) docker_checks
# Also deletes volumes.
# Comment the line in the file to keep them.


rm_volumes:
	./$(SCRIPTS_DIR)/$(RM_VOLUMES_SH)


download:
	./$(SCRIPTS_DIR)/manage_app_directory.sh

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
	@echo "\nTo use Adminer, follow these steps:"
	@echo "1. Open your browser and go to: http://localhost:8080"
	@echo "2. Fill in the following connection details:"
	@echo "   - System: PostgreSQL"
	@echo "   - Server: postgres"
	@echo "   - Username: nstoutze"
	@echo "   - Password: mysecretpassword"
	@echo "   - Database: piscineds"


ex00: up
	$(PSQL)


ex01-setup: up
	@echo "Creating table $(EXAMPLE_TABLE)..."
	$(PSQL) -c "\
	CREATE TABLE IF NOT EXISTS $(EXAMPLE_TABLE) ( \
		event_time TIMESTAMP NOT NULL, \
		event_type VARCHAR(50), \
		product_id BIGINT, \
		category_id NUMERIC, \
		category_code VARCHAR(255), \
		brand VARCHAR(255), \
		price NUMERIC(10, 2), \
		user_id BIGINT, \
		user_session VARCHAR(255) \
	);"

	$(DOCKER_PYTHON) $(CHECK_LOG_TABLE) $(EXAMPLE_TABLE)

	@echo "Inserting data into $(EXAMPLE_TABLE) if empty..."
	$(PSQL) -c "\
	INSERT INTO $(EXAMPLE_TABLE) (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) \
	SELECT * FROM ( \
	    VALUES \
	    ('2022-10-01 00:00:00'::TIMESTAMP, 'cart', 5773203, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	    ('2022-10-01 00:00:03'::TIMESTAMP, 'cart', 5773353, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5723490, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5881589, 215119107051219712, NULL, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	    ('2022-10-01 00:00:15'::TIMESTAMP, 'cart', 5881449, 148758000513522845952, NULL, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	    ('2022-10-01 00:00:16'::TIMESTAMP, 'cart', 5857269, 1487580005134238464, NULL, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f') \
	) AS new_data \
	WHERE NOT EXISTS (SELECT 1 FROM $(EXAMPLE_TABLE))";

	@echo "Logging insertion into table logs..."
	$(PSQL) -c "\
	BEGIN; \
	INSERT INTO logs (table_name, last_import) VALUES ('$(EXAMPLE_TABLE)', NOW()) \
	ON CONFLICT (table_name) DO UPDATE SET last_import = NOW(); \
	COMMIT;"

	@$(MAKE) adminer

ex01-clean:
	@echo "Dropping table $(EXAMPLE_TABLE)..."
	$(PSQL) -c "DROP TABLE IF EXISTS $(EXAMPLE_TABLE);"


ex02-import_data: up
	@if [ -f $(EX02_SQL) ]; then \
		echo "Copying SQL file to container..."; \
		cat $(EX02_SQL) | docker exec -i $(POSTGRES_CONTAINER) sh -c 'cat > /create_and_import.sql'; \
		echo "Executing SQL file to create table and import data..."; \
		docker exec -i $(POSTGRES_CONTAINER) psql $(PSQL_OPTIONS) -f /create_and_import.sql; \
		echo "Table created and data imported successfully!"; \
	else \
		echo "Error: $(EX02_SQL) not found."; \
	fi
#	tee could have been necessary in during the copy into the container,
#	instead of cat (the second one), to avoid errors ; however, cat works well.
#	-f is a psql option for running .sql files

ex02-verify_data: up
	$(PSQL) -c "\
	SELECT COUNT(*) FROM data_2022_dec; \
	SELECT * FROM data_2022_dec LIMIT 10;"

ex02-clean: up
	$(PSQL) -c "DROP TABLE IF EXISTS data_2022_dec;"
	@$(DOCKER_EXEC) $(POSTGRES_CONTAINER) rm -f \
		create_and_import.sql \
		data_2022_dec.csv \
		|| echo "Files already removed or not found."


ex03: up ex02-clean
	$(DOCKER_PYTHON) /$(APP_DIR)/$(EX03_PY)


ex04: up
	$(DOCKER_PYTHON) /$(APP_DIR)/$(EX04_PY)


#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
