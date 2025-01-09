
COMPOSE_FILE = docker-compose.yaml
DOCKER_RUN = docker run -it --rm --network="host" postgres:latest psql -U nstoutze -d piscineds -h localhost -W
DOCKER_EXEC = docker exec -it postgres_piscineds
BROWSER = xdg-open
TABLE_NAME = example_table

.PHONY: up psql down adminer ex00 ex01-setup ex01-clean


up:
	docker-compose -f $(COMPOSE_FILE) up -d


psql:
	$(DOCKER_RUN)


down:
	docker-compose -f $(COMPOSE_FILE) down --volumes


adminer:
	google-chrome --new-tab http://localhost:8080


ex00: up psql down


ex01-setup: up
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in {1..10}; do \
	    $(DOCKER_EXEC) psql -U nstoutze -d piscineds -c "SELECT 1" && break || sleep 2; \
	done
	@echo "Creating table $(TABLE_NAME)..."
	$(DOCKER_EXEC) psql -U nstoutze -d piscineds -c "\
	CREATE TABLE $(TABLE_NAME) ( \
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

	@echo "Inserting data into $(TABLE_NAME)..."
	$(DOCKER_EXEC) psql -U nstoutze -d piscineds -c "\
	INSERT INTO $(TABLE_NAME) (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) VALUES \
	('2022-10-01 00:00:00', 'cart', 5773203, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:03', 'cart', 5773353, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:07', 'cart', 5723490, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'), \
	('2022-10-01 00:00:07', 'cart', 5881589, 215119107051219712, NULL, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	('2022-10-01 00:00:15', 'cart', 5881449, 148758000513522845952, NULL, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'), \
	('2022-10-01 00:00:16', 'cart', 5857269, 1487580005134238464, NULL, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f');"
	@echo "Table $(TABLE_NAME) created and populated successfully!"
	@echo "\nTo use Adminer, follow these steps:"
	@echo "1. Open your browser and go to: http://localhost:8080"
	@echo "2. Fill in the following connection details:"
	@echo "   - System: PostgreSQL"
	@echo "   - Server: postgres"
	@echo "   - Username: nstoutze"
	@echo "   - Password: mysecretpassword"
	@echo "   - Database: piscineds"


ex01-clean:
	@echo "Dropping table $(TABLE_NAME)..."
	$(DOCKER_EXEC) psql -U nstoutze -d piscineds -c "DROP TABLE IF EXISTS $(TABLE_NAME);"
