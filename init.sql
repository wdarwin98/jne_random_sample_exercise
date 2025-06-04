CREATE TABLE jne_logistics_data (
    tracking_number TEXT,
    created_at TEXT,
    delivery_time TEXT,
    current_status TEXT,
    delivery_steps TEXT,
    origin TEXT,
    destination TEXT,
    actual_lat TEXT,
    actual_lon TEXT,
    expected_lat TEXT,
    expected_lon TEXT,
    recipient_name TEXT,
    sender_name TEXT,
    recipient_phone TEXT,
    weight_kg TEXT,
    dimensions_cm TEXT,
    is_duplicate TEXT
);



-- Load the CSV file into the table

SET datestyle = 'ISO, DMY';

COPY jne_logistics_data
FROM '/docker-entrypoint-initdb.d/raw_data_mart.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  ENCODING 'UTF8'
);
