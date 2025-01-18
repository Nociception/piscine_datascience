COMPOSE_FILE = docker-compose.yaml
POSTGRES_RUN = docker exec -it postgres_piscineds psql -U nstoutze -d piscineds -h localhost
DOCKER_RUN = docker run -it --rm --network="host" postgres:latest psql -U nstoutze -d piscineds -h localhost -W
POSTGRES_CONTAINER_NAME = postgres_piscineds
PYTHON_CONTAINER_NAME = python
DOCKER_EXEC = docker exec -it
PSQL_COMMAND = psql -U nstoutze -d piscineds -c
OUTPUT_ZIP = subject.zip
SUBJECT_DIRNAME = subject
APP_DIR_NAME = app
EXAMPLE_TABLE_NAME = example_table

URL_ZIP = https://cdn.intra.42.fr/document/document/23499/subject.zip
URL_FEB_CSV = https://cdn.intra.42.fr/document/document/28095/data_2023_feb.csv
FEB_CSV = data_2023_feb.csv
EX01_PYTHON_FILE = customers_table.py

EX02_PYTHON_FILE = remove_duplicates.py

.PHONY: up psql down env withoutpw manage_app_directory end fclean\
		docker_checks postgres_container python_container update_py_files\
		ex00 \
		ex01-setup ex01-clean download unzip\
		ex02 \
		ex03 \


up: unzip env update_py_files
	docker-compose -f $(COMPOSE_FILE) up -d
	$(MAKE) docker_checks

update_py_files:
	echo "Checking for differences in Python files..."

	@DIFF_EX01=0; DIFF_EX02=0; \
	
	@if ! diff ex01/$(EX01_PYTHON_FILE) app/$(EX01_PYTHON_FILE) > /dev/null; then \
		echo "Differences found in ex01 files. Updating..."; \
		cp ex01/$(EX01_PYTHON_FILE) app/$(EX01_PYTHON_FILE); \
		DIFF_EX01=1; \
	fi; \
	
	@if ! diff ex02/$(EX02_PYTHON_FILE) app/$(EX02_PYTHON_FILE) > /dev/null; then \
		echo "Differences found in ex02 files. Updating..."; \
		cp ex02/$(EX02_PYTHON_FILE) app/$(EX02_PYTHON_FILE); \
		DIFF_EX02=1; \
	fi; \
	
	@if [ $$DIFF_EX01 -eq 1 ] || [ $$DIFF_EX02 -eq 1 ]; then \
		echo "Restarting Python container..."; \
		docker kill $(PYTHON_CONTAINER_NAME); \
		docker-compose up -d $(PYTHON_CONTAINER_NAME); \
		echo "$(PYTHON_CONTAINER_NAME) container restarted."; \
	else \
		echo "No differences found. No updates nor container restart needed."; \
	fi


docker_checks:
	docker ps
	docker volume ls

postgres_container:
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) bash

python_container:
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) bash

psql:
	$(POSTGRES_RUN)

down:
	docker kill $(PYTHON_CONTAINER_NAME)
	docker-compose -f $(COMPOSE_FILE) down
	$(MAKE) docker_checks
#	docker volume rm $(docker volume ls -q)

manage_app_directory:
	@if [ -d $(APP_DIR_NAME) ]; then \
		echo "$(APP_DIR_NAME) already exists. Skipping creation."; \
	else \
		mkdir app ; \
	fi

	@if [ -f $(APP_DIR_NAME)/$(EX01_PYTHON_FILE) ]; then \
		echo "$(EX01_PYTHON_FILE) already in $(APP_DIR_NAME); skipping copy."; \
	else \
		cp ex01/$(EX01_PYTHON_FILE) $(APP_DIR_NAME)/ ; \
		echo "ex01/$(EX01_PYTHON_FILE) copied into the $(APP_DIR_NAME) directory."; \
	fi

	@if [ -f $(APP_DIR_NAME)/$(EX02_PYTHON_FILE) ]; then \
		echo "$(EX02_PYTHON_FILE) already in $(APP_DIR_NAME); skipping copy."; \
	else \
		cp ex02/$(EX02_PYTHON_FILE) $(APP_DIR_NAME)/ ; \
		echo "ex02/$(EX02_PYTHON_FILE) copied into the $(APP_DIR_NAME) directory."; \
	fi

	@if [ -f $(APP_DIR_NAME)/requirements.txt ]; then \
		echo "requirements.txt already in $(APP_DIR_NAME); skipping copy."; \
	else \
		cp requirements.txt $(APP_DIR_NAME)/ ; \
		echo "requirements.txt copied into the $(APP_DIR_NAME) directory."; \
	fi

download: manage_app_directory
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
	@if [ -d $(SUBJECT_DIRNAME) ]; then \
		rm -rf $(SUBJECT_DIRNAME); \
		echo "$(SUBJECT_DIRNAME) directory deleted."; \
	else \
		echo "$(SUBJECT_DIRNAME) directory does not exist."; \
	fi

	@if [ -f $(OUTPUT_ZIP) ]; then \
		rm -rf $(OUTPUT_ZIP); \
		echo "$(OUTPUT_ZIP) file deleted."; \
	else \
		echo "$(OUTPUT_ZIP) file does not exist."; \
	fi

	@if [ -d $(APP_DIR_NAME) ]; then \
		rm -rf $(APP_DIR_NAME); \
		echo "$(APP_DIR_NAME) directory deleted."; \
	else \
		echo "$(APP_DIR_NAME) directory does not exist."; \
	fi

	@if [ -f .env ]; then \
		rm .env; \
		echo ".env file deleted."; \
	else \
		echo ".env file does not exist."; \
	fi

	docker-compose -f $(COMPOSE_FILE) down --volumes

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

withoutpw:
	$(DOCKER_EXEC) -U nstoutze -d piscineds -h localhost

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
		echo ".env file created successfully."; \
	fi


ex00-setup: up
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in {1..10}; do \
		$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) $(PSQL_COMMAND) "SELECT 1" && break || sleep 2; \
	done

	@echo "Creating table $(EXAMPLE_TABLE_NAME)..."
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) $(PSQL_COMMAND) "\
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
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) $(PSQL_COMMAND) "\
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
	$(DOCKER_EXEC) $(POSTGRES_CONTAINER_NAME) $(PSQL_COMMAND) "DROP TABLE IF EXISTS $(EXAMPLE_TABLE_NAME);"


ex01: unzip up ex00-clean
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) python3 /app/$(EX01_PYTHON_FILE)


ex02: up
	$(DOCKER_EXEC) $(PYTHON_CONTAINER_NAME) python3 /app/$(EX02_PYTHON_FILE)


#BIG CLEAN
# docker stop $(docker ps -aq)
# docker rm -f $(docker ps -aq)
# docker volume rm $(docker volume ls -q)
# docker rmi -f $(docker images -aq)
# docker ps -a
# docker volume ls
