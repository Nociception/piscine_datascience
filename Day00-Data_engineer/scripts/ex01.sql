	@echo "Creating table $(EXAMPLE_TABLE)..."
-- J'imagine que l'on ne peut pas ecrire des logs sur la console avec un fichier sql
CREATE TABLE IF NOT EXISTS $(EXAMPLE_TABLE) (
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    product_id BIGINT,
    category_id NUMERIC,
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price NUMERIC(10, 2),
    user_id BIGINT,
    user_session VARCHAR(255)
);

-- ici, il faut checker la log table, mais sans l'outil python dedie,
-- car je doute que sql puisse appeler un script python

	@echo "Inserting data into $(EXAMPLE_TABLE) if empty..."
-- Meme remarque sur les logs
INSERT INTO $(EXAMPLE_TABLE) (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
SELECT * FROM (
    VALUES
    ('2022-10-01 00:00:00'::TIMESTAMP, 'cart', 5773203, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),
    ('2022-10-01 00:00:03'::TIMESTAMP, 'cart', 5773353, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),
    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5723490, 1487580005134238464, NULL, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),
    ('2022-10-01 00:00:07'::TIMESTAMP, 'cart', 5881589, 215119107051219712, NULL, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'),
    ('2022-10-01 00:00:15'::TIMESTAMP, 'cart', 5881449, 148758000513522845952, NULL, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'),
    ('2022-10-01 00:00:16'::TIMESTAMP, 'cart', 5857269, 1487580005134238464, NULL, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f')
) AS new_data
WHERE NOT EXISTS (SELECT 1 FROM $(EXAMPLE_TABLE));

	@echo "Logging insertion into table logs..."
-- Meme remarque sur les logs

BEGIN;
INSERT INTO logs (table_name, last_import) VALUES ('$(EXAMPLE_TABLE)', NOW())
ON CONFLICT (table_name) DO UPDATE SET last_import = NOW();
COMMIT;