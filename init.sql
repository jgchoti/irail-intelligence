-- Create databases if they don't exist
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE irail_intelligence'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'irail_intelligence')\gexec

SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

SELECT 'CREATE DATABASE mlflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

-- Connect to irail_intelligence database
\c irail_intelligence;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing table if it exists
DROP TABLE IF EXISTS departures_raw CASCADE;

-- Raw departures table
CREATE TABLE departures_raw (
    id SERIAL,
    station VARCHAR(100) NOT NULL,
    train_id VARCHAR(50) NOT NULL,
    vehicle VARCHAR(50),
    platform VARCHAR(10),
    
    scheduled_time TIMESTAMP NOT NULL,
    delay_seconds INT DEFAULT 0,
    cancelled BOOLEAN DEFAULT FALSE,
    
    destination VARCHAR(100),
    
    collected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (id, scheduled_time)
);

-- Convert to hypertable 
SELECT create_hypertable('departures_raw', 'scheduled_time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_station ON departures_raw(station, scheduled_time DESC);
CREATE INDEX IF NOT EXISTS idx_train_id ON departures_raw(train_id);
CREATE INDEX IF NOT EXISTS idx_collected_at ON departures_raw(collected_at DESC);