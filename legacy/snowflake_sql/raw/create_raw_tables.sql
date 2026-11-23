-- sql/raw/create_raw_tables.sql
-- Run this once to set up the RAW schema.
-- RAW = untouched API responses stored as VARIANT (Snowflake's JSON type).
-- We never UPDATE or DELETE here — only INSERT.
-- If a transformation breaks, we replay from RAW without re-calling the API.

CREATE DATABASE IF NOT EXISTS EIA_ENERGY;

CREATE SCHEMA IF NOT EXISTS EIA_ENERGY.RAW;

-- Retail electricity prices by state and sector
CREATE TABLE IF NOT EXISTS EIA_ENERGY.RAW.RETAIL_PRICES (
    record_id       NUMBER AUTOINCREMENT PRIMARY KEY,
    payload         VARIANT,          -- raw JSON record exactly as returned by EIA
    source_file     VARCHAR,          -- s3 key this record came from
    ingested_at     TIMESTAMP_NTZ DEFAULT SYSDATE()
);

-- Electricity generation by fuel type
CREATE TABLE IF NOT EXISTS EIA_ENERGY.RAW.GENERATION (
    record_id       NUMBER AUTOINCREMENT PRIMARY KEY,
    payload         VARIANT,
    source_file     VARCHAR,
    ingested_at     TIMESTAMP_NTZ DEFAULT SYSDATE()
);

-- Hourly RTO/balancing authority demand
CREATE TABLE IF NOT EXISTS EIA_ENERGY.RAW.RTO_DEMAND (
    record_id       NUMBER AUTOINCREMENT PRIMARY KEY,
    payload         VARIANT,
    source_file     VARCHAR,
    ingested_at     TIMESTAMP_NTZ DEFAULT SYSDATE()
);
