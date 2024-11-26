-- Table for raw sensor data
CREATE TABLE raw_sensor_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    light FLOAT,
    CO2 FLOAT,
    occupancy INTEGER
);

-- Table for processed sensor data
CREATE TABLE processed_sensor_data (
    id SERIAL PRIMARY KEY,
    avg_temperature FLOAT,
    avg_humidity FLOAT,
    avg_light FLOAT,
    avg_CO2 FLOAT,
    occupancy_rate FLOAT,
    timestamp TIMESTAMP
);
