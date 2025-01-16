COMPOSE_FILE = docker-compose.yaml
DOCKER_RUN = docker run -it --rm --network="host" postgres:latest psql -U nstoutze -d piscineds -h localhost -W
DOCKER_EXEC = docker exec -it postgres_piscineds
PSQL_COMMAND = psql -U nstoutze -d piscineds -c
URL_ZIP = https://cdn.intra.42.fr/document/document/23499/subject.zip
OUTPUT_ZIP = subject.zip
SUBJECT_DIRNAME = subject
URL_FEB_CSV = https://cdn.intra.42.fr/document/document/28095/data_2023_feb.csv
FEB_CSV = data_2023_feb.csv


.PHONY: up psql down env withoutpw download unzip \
		ex00-setup ex00-clean \
		ex01 \
		ex02 \
		ex03 \


up: unzip
	docker-compose -f $(COMPOSE_FILE) up -d
	$(MAKE) docker_checks

docker_checks:
	docker ps
	docker volume ls

container:
	$(DOCKER_EXEC) bash

psql:
	docker exec -it postgres_piscineds psql -U nstoutze -d piscineds -h localhost


down:
	docker-compose -f $(COMPOSE_FILE) down --volumes
	$(MAKE) docker_checks

#	docker volume rm $(docker volume ls -q)

download:
	@if [ -f $(OUTPUT_ZIP) ]; then \
		echo "$(OUTPUT_ZIP) already exists. Skipping download."; \
	else \
		echo "Downloading $(OUTPUT_ZIP) from $(URL_ZIP)..."; \
		curl -o $(OUTPUT_ZIP) -L $(URL_ZIP); \
		echo "Download complete: $(OUTPUT_ZIP)"; \
	fi

	@if [ -f $(FEB_CSV) ] || [ -f $(SUBJECT_DIRNAME)/customer/$(FEB_CSV) ]; then \
		echo "$(FEB_CSV) already exists. Skipping download."; \
	else \
		echo "Downloading $(FEB_CSV) from $(URL_FEB_CSV)..."; \
		curl -o $(FEB_CSV) -L $(URL_FEB_CSV); \
		echo "Download complete: $(FEB_CSV)"; \
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

	@if [ -f $(FEB_CSV) ]; then \
		mv $(FEB_CSV) $(SUBJECT_DIRNAME)/customer/ ; \
		echo "$(FEB_CSV) properly moved with the other .csv files into $(SUBJECT_DIRNAME)/customer/" ; \
	elif [ -f $(SUBJECT_DIRNAME)/customer/$(FEB_CSV) ]; then \
		echo "$(FEB_CSV) is already in $(SUBJECT_DIRNAME)/customer"; \
	else \
		echo "$(FEB_CSV) does not exist. You must download it for the exercises"; \
	fi

fclean:
	rm -rf subject

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
		echo "POSTGRES_CONTAINER_NAME=postgres_piscineds" >> .env; \
		echo ".env file created successfully."; \
	fi
# Unable to make the makefile work with a heredoc.


ex00-setup: up
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(PSQL_COMMAND) "SELECT 1" && break || sleep 2; \
	done

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

ex00-clean:
	@echo "Dropping table $(EXAMPLE_TABLE_NAME)..."
	$(DOCKER_EXEC) $(PSQL_COMMAND) "DROP TABLE IF EXISTS $(EXAMPLE_TABLE_NAME);"


ex01: unzip
	cd ex01 ; python3 customers_table.py



#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
# docker ps -a
# docker volume ls
