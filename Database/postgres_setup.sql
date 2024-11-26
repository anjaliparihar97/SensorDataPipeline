CREATE DATABASE occupancy_data;

CREATE USER pipeline_user WITH PASSWORD 'password123';
GRANT ALL PRIVILEGES ON DATABASE occupancy_data TO pipeline_user;
