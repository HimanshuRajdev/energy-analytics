-- sql/staging/create_staging_tables.sql
-- STAGING = flattened, typed, deduplicated data from RAW.
-- We use LATERAL FLATTEN to unpack the VARIANT JSON into proper columns.
-- This is where type casting and deduplication happen.

CREATE SCHEMA IF NOT EXISTS EIA_ENERGY.STAGING;

-- Staged retail prices
CREATE TABLE IF NOT EXISTS EIA_ENERGY.STAGING.RETAIL_PRICES (
    period          VARCHAR(7),       -- YYYY-MM
    state_id        VARCHAR(10),      -- e.g. 'TX', 'CA'
    state_name      VARCHAR(100),
    sector_id       VARCHAR(20),      -- e.g. 'RES', 'COM', 'IND'
    sector_name     VARCHAR(100),
    price_cents_kwh FLOAT,            -- cents per kilowatt-hour
    sales_mwh       FLOAT,            -- million kilowatt hours sold
    customers       BIGINT,           -- number of customers
    ingested_at     TIMESTAMP_NTZ,
    PRIMARY KEY (period, state_id, sector_id)
);

-- Staged generation by fuel type
CREATE TABLE IF NOT EXISTS EIA_ENERGY.STAGING.GENERATION (
    period              VARCHAR(7),
    state_id            VARCHAR(10),
    fuel_type_code      VARCHAR(50),   -- e.g. 'NG' (natural gas), 'SUN', 'WND'
    fuel_type_desc      VARCHAR(100),
    generation_mwh      FLOAT,
    ingested_at         TIMESTAMP_NTZ,
    PRIMARY KEY (period, state_id, fuel_type_code)
);

-- Staged RTO demand (hourly)
CREATE TABLE IF NOT EXISTS EIA_ENERGY.STAGING.RTO_DEMAND (
    period          TIMESTAMP_NTZ,    -- hourly timestamp
    region_id       VARCHAR(50),
    region_name     VARCHAR(100),
    demand_mwh      FLOAT,
    ingested_at     TIMESTAMP_NTZ,
    PRIMARY KEY (period, region_id)
);
