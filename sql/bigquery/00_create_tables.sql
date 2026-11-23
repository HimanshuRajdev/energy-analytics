-- BigQuery setup. Run once (the DAG also runs it on every load, it is idempotent).
-- {{project}} is replaced by src/bq_client.py with your GCP project id.
-- Free tier: 10 GB storage and 1 TB of queries per month. RAW only ever holds the
-- latest load (it is truncated before each run), so storage stays small.

CREATE SCHEMA IF NOT EXISTS `{{project}}.eia_raw`;
CREATE SCHEMA IF NOT EXISTS `{{project}}.eia_staging`;
CREATE SCHEMA IF NOT EXISTS `{{project}}.eia_marts`;

-- RAW keeps each API record as a JSON string, exactly as EIA returned it
CREATE TABLE IF NOT EXISTS `{{project}}.eia_raw.retail_prices` (
  payload     STRING,
  source_file STRING,
  ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS `{{project}}.eia_raw.generation` (
  payload     STRING,
  source_file STRING,
  ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS `{{project}}.eia_raw.rto_demand` (
  payload     STRING,
  source_file STRING,
  ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{project}}.eia_staging.retail_prices` (
  period          STRING,      -- YYYY-MM
  state_id        STRING,
  state_name      STRING,
  sector_id       STRING,
  sector_name     STRING,
  price_cents_kwh FLOAT64,
  sales_mwh       FLOAT64,
  customers       INT64,
  ingested_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{project}}.eia_staging.generation` (
  period         STRING,
  state_id       STRING,
  fuel_type_code STRING,
  fuel_type_desc STRING,
  generation_mwh FLOAT64,
  ingested_at    TIMESTAMP
);

-- Partitioned by day and clustered by region so the mart rebuilds scan less data
CREATE TABLE IF NOT EXISTS `{{project}}.eia_staging.rto_demand` (
  period      TIMESTAMP,
  region_id   STRING,
  region_name STRING,
  demand_mwh  FLOAT64,
  ingested_at TIMESTAMP
)
PARTITION BY DATE(period)
CLUSTER BY region_id;


-- BigQuery's no-billing sandbox creates datasets with a 60 day expiry on tables and partitions.
-- With the daily-partitioned demand table, that silently deletes any row older than 60 days.
-- Clear it everywhere. Safe to repeat.
ALTER SCHEMA `{{project}}.eia_raw`
  SET OPTIONS (default_table_expiration_days = NULL, default_partition_expiration_days = NULL);
ALTER SCHEMA `{{project}}.eia_staging`
  SET OPTIONS (default_table_expiration_days = NULL, default_partition_expiration_days = NULL);
ALTER SCHEMA `{{project}}.eia_marts`
  SET OPTIONS (default_table_expiration_days = NULL, default_partition_expiration_days = NULL);

ALTER TABLE `{{project}}.eia_raw.retail_prices`     SET OPTIONS (expiration_timestamp = NULL);
ALTER TABLE `{{project}}.eia_raw.generation`        SET OPTIONS (expiration_timestamp = NULL);
ALTER TABLE `{{project}}.eia_raw.rto_demand`        SET OPTIONS (expiration_timestamp = NULL);
ALTER TABLE `{{project}}.eia_staging.retail_prices` SET OPTIONS (expiration_timestamp = NULL);
ALTER TABLE `{{project}}.eia_staging.generation`    SET OPTIONS (expiration_timestamp = NULL);
ALTER TABLE `{{project}}.eia_staging.rto_demand`    SET OPTIONS (expiration_timestamp = NULL, partition_expiration_days = NULL);
