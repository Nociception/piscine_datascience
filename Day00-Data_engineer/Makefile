COMPOSE_FILE = docker-compose.yaml
DOCKER_EXEC = docker exec -it
POSTGRES_CONTAINER_NAME = postgres_piscineds
PSQL_OPTIONS = -U nstoutze -d piscineds -h localhost
PSQL = $(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) psql $(PSQL_OPTIONS)
PYTHON_CONTAINER_NAME = python
EXAMPLE_TABLE_NAME = example_table
URL = https://cdn.intra.42.fr/document/document/23499/subject.zip
OUTPUT_ZIP = subject.zip
SUBJECT_DIRNAME = subject
APP_DIR = app
REQ_FILE = requirements.txt
SCRIPTS_DIR = scripts
SQL_FILE = ex02/table.sql
CSV_FILE = subject/customer/data_2022_dec.csv
CSV_CONTAINER_PATH = /data_2022_dec.csv
EX03_PYTHON_FILE = automatic_table.py
EX04_PYTHON_FILE = items_table.py


.PHONY: up psql down env end fclean \
		docker_checks postgres_container python_container \
		ex00 \
		ex01-setup ex01-clean \
		download unzip ex02-import_data ex02-verify_data ex02-clean \
		ex03 \
		ex04 \


up: env unzip
	./$(SCRIPTS_DIR)/update_py_files.sh
	docker-compose -f $(COMPOSE_FILE) up -d
	$(MAKE) docker_checks
	$(MAKE) wait_for_postgres


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
		echo "POSTGRES_CONTAINER_NAME=postgres_piscineds" >> .env; \
		echo "PYTHON_CONTAINER_NAME=python" >> .env; \
		echo "APP_DIR=$(APP_DIR)" >> .env; \
		echo "REQ_FILE=$(REQ_FILE)" >> .env; \
		echo ".env file created successfully."; \
	fi
# Unable to make the makefile work with a heredoc.


docker_checks:
	docker ps
	docker volume ls


psql: wait_for_postgres
	$(PSQL)


postgres_container:
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) bash


python_container:
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) bash


wait_for_postgres:
	@echo "Checking if PostgreSQL is ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) pg_isready $(PSQL_OPTIONS) && break || sleep 2; \
	done
	@echo "PostgreSQL is ready!"


down:
	docker kill $(PYTHON_CONTAINER_NAME)
	docker-compose -f $(COMPOSE_FILE) down
	$(MAKE) docker_checks
	@echo "\nrun this: docker volume rm \$$(docker volume ls -q)"


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
	@if [ -d $(SUBJECT_DIRNAME) ]; then \
		echo "Directory $(SUBJECT_DIRNAME) already exists. Skipping extraction."; \
	elif [ -f $(OUTPUT_ZIP) ]; then \
		echo "Extracting $(OUTPUT_ZIP)..."; \
		unzip -o $(OUTPUT_ZIP); \
		echo "Extraction complete: files are in $(SUBJECT_DIRNAME)/"; \
	else \
		echo "$(OUTPUT_ZIP) not found. Please run 'make download' first."; \
	fi


fclean:
	rm -rf $(SUBJECT_DIRNAME) $(OUTPUT_ZIP) $(APP_DIR_NAME) .env || true


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
	@echo "Creating table $(EXAMPLE_TABLE_NAME)..."
	$(PSQL) -c "\
	CREATE TABLE $(EXAMPLE_TABLE_NAME) ( \
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

	@echo "Inserting data into $(EXAMPLE_TABLE_NAME)..."
	$(PSQL) -c "\
	INSERT INTO $(EXAMPLE_TABLE_NAME) (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) VALUES \
	('2022-10-01 00:00:00', 'cart', 5773203, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:03', 'cart', 5773353, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:07', 'cart', 5723490, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:07', 'cart', 5881589, 215119107051219712, NULL, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	('2022-10-01 00:00:15', 'cart', 5881449, 148758000513522845952, NULL, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	('2022-10-01 00:00:16', 'cart', 5857269, 1487580005134238464, NULL, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f');"
	@echo "Table $(EXAMPLE_TABLE_NAME) created and populated successfully!"

	@$(MAKE) adminer

ex01-clean:
	@echo "Dropping table $(EXAMPLE_TABLE_NAME)..."
	$(PSQL) -c "DROP TABLE IF EXISTS $(EXAMPLE_TABLE_NAME);"


ex02-import_data: up ex01-clean
	@if [ -f $(SQL_FILE) ]; then \
		echo "Copying SQL file to container..."; \
		cat $(SQL_FILE) | docker exec -i $(POSTGRES_CONTAINER_NAME) sh -c 'tee /create_and_import.sql > /dev/null'; \
		echo "Executing SQL file to create table and import data..."; \
		docker exec -i $(POSTGRES_CONTAINER_NAME) psql $(PSQL_OPTIONS) -f /create_and_import.sql; \
		echo "Table created and data imported successfully!"; \
	else \
		echo "Error: $(SQL_FILE) not found."; \
	fi
#	-f is a psql option for running .sql files

ex02-verify_data: up
	$(PSQL) -c "\
	SELECT COUNT(*) FROM data_2022_dec; \
	SELECT * FROM data_2022_dec LIMIT 10;"

ex02-clean: up
	$(PSQL) -c "DROP TABLE IF EXISTS data_2022_dec;"
	@$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) rm -f \
		create_and_import.sql \
		data_2022_dec.csv \
		|| echo "Files already removed or not found."


ex03: up ex02-clean
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) python3 /$(APP_DIR)/$(EX03_PYTHON_FILE)


ex04: up
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) python3 /$(APP_DIR)/$(EX04_PYTHON_FILE)


#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
