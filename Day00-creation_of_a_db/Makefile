COMPOSE_FILE = docker-compose.yaml
DOCKER_RUN = docker run -it --rm --network="host" postgres:latest psql -U nstoutze -d piscineds -h localhost -W
DOCKER_EXEC = docker exec -it postgres_piscineds
PSQL_COMMAND = psql -U nstoutze -d piscineds -c
EXAMPLE_TABLE_NAME = example_table
URL = https://cdn.intra.42.fr/document/document/23499/subject.zip
OUTPUT_ZIP = subject.zip
SUBJECT_DIRNAME = subject

SQL_FILE = ex02/table.sql
CSV_FILE = subject/customer/data_2022_dec.csv
CSV_CONTAINER_PATH = /data_2022_dec.csv




.PHONY: up psql down ex00 ex01-setup ex01-clean download unzip env withoutpw

up:
	docker-compose -f $(COMPOSE_FILE) up -d


adminer:
	@echo "\nTo use Adminer, follow these steps:"
	@echo "1. Open your browser and go to: http://localhost:8080"
	@echo "2. Fill in the following connection details:"
	@echo "   - System: PostgreSQL"
	@echo "   - Server: postgres"
	@echo "   - Username: nstoutze"
	@echo "   - Password: mysecretpassword"
	@echo "   - Database: piscineds"

withoutpw:
	$(DOCKER_EXEC) -U nstoutze -d piscineds -h localhost
# Checks the pg_hba.conf file to know what method is used to authenticate during a db connection:
# docker exec -it postgres_piscineds sh
#	cat /var/lib/postgresql/data/pg_hba.conf


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
		echo ".env file created successfully."; \
	fi
# Unable to make the makefile work with a heredoc.

psql:
	$(DOCKER_RUN)

down:
	docker-compose -f $(COMPOSE_FILE) down --volumes

ex00: up psql down


ex01-setup: up
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(PSQL_COMMAND) "SELECT 1" && break || sleep 2; \
	done
#		Reminder : DOCKER_EXEC = docker exec -it postgres_piscineds
#			It then starts a session inside the postgres docker container.
#			Everything next will be handled from the container.
#			-U for specifying the user, -d the database
#			-c for a command ; here "SELECT 1" :
#				Asks the db to return 1, if it is available for.
#				The result is not related to any table, data or existing structure.
#				Mainly used to test server connection.
#				The command can then lead to an error such as (most common ones):
#					psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory
#					FATAL: password authentication failed for user "nstoutze"
#					FATAL: database "piscineds" does not exist
#				If one of these occurs, the command part result into FALSE, and the break is not reached.
#	About the $(DOCKER_EXEC) commands in a Makefile :
#		Each of them are related to a specific session running as long as the command has still something to do.
#		After a command running, the docker session is over.
#		That is why it is possible to read many $(DOCKER_EXEC) calls in this Makefile.

	@echo "Creating table $(EXAMPLE_TABLE_NAME)..."
	$(DOCKER_EXEC) $(PSQL_COMMAND) "\
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
	$(DOCKER_EXEC) $(PSQL_COMMAND) "\
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
	$(DOCKER_EXEC) $(PSQL_COMMAND) "DROP TABLE IF EXISTS $(EXAMPLE_TABLE_NAME);"


download:
	@if [ -f $(OUTPUT_ZIP) ]; then \
		echo "$(OUTPUT_ZIP) already exists. Skipping download."; \
	else \
		echo "Downloading $(OUTPUT_ZIP) from $(URL)..."; \
		curl -o $(OUTPUT_ZIP) -L $(URL); \
		echo "Download complete: $(OUTPUT_ZIP)"; \
	fi
#	-L makes curl follow potential (and frequent) URL chains, in order to reach the targeted file.

unzip:
	@if [ -d $(SUBJECT_DIRNAME) ]; then \
		echo "Directory $(SUBJECT_DIRNAME) already exists. Skipping extraction."; \
	elif [ -f $(OUTPUT_ZIP) ]; then \
		echo "Extracting $(OUTPUT_ZIP)..."; \
		unzip -o $(OUTPUT_ZIP); \
		echo "Extraction complete: files are in $(SUBJECT_DIRNAME)/"; \
	else \
		echo "$(OUTPUT_ZIP) not found. Please run 'make download' first."; \
	fi

ex02-import_data:
	@if [ -f $(SQL_FILE) ] && [ -f $(CSV_FILE) ]; then \
		echo "Copying SQL file to container..."; \
		docker cp $(SQL_FILE) postgres_piscineds:/create_and_import.sql; \
		echo "Copying CSV file to container..."; \
		docker cp $(CSV_FILE) postgres_piscineds:$(CSV_CONTAINER_PATH); \
		echo "Fixing ownership of the CSV file in the container..."; \
		$(DOCKER_EXEC) chown postgres:postgres $(CSV_CONTAINER_PATH); \
		echo "Executing SQL file to create table and import data..."; \
		$(DOCKER_EXEC) psql -U nstoutze -d piscineds -f /create_and_import.sql; \
		echo "Table created and data imported successfully!"; \
	else \
		echo "Error: $(SQL_FILE) or $(CSV_FILE) not found."; \
	fi
# -f is a psql option for running .sql files

ex02-verify_data:
	$(DOCKER_EXEC) $(PSQL_COMMAND) "\
	SELECT COUNT(*) FROM data_2022_dec; \
	SELECT * FROM data_2022_dec LIMIT 10;"

ex02-clean:
	$(DOCKER_EXEC) $(PSQL_COMMAND) "DROP TABLE IF EXISTS data_2022_dec;"



#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
# docker ps -a
# docker volume ls
