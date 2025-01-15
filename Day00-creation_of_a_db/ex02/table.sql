CREATE TABLE IF NOT EXISTS data_2022_dec (
    event_time TIMESTAMP WITH TIME ZONE,
    event_type VARCHAR(50),
    product_id INT,
    price NUMERIC(10, 2),
    user_id BIGINT,
    user_session UUID
);

COPY data_2022_dec (event_time, event_type, product_id, price, user_id, user_session)
FROM '/data_2022_dec.csv'
DELIMITER ','
CSV HEADER;
