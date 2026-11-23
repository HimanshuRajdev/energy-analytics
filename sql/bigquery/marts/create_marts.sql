-- MARTS = analytics-ready tables for the dashboard.
-- Same logic as the old Snowflake version. Runs as one BigQuery script.

-- Monthly retail prices with a 12-month rolling z-score. |z| > 2.5 is an anomaly.
CREATE OR REPLACE TABLE `{{project}}.eia_marts.fact_retail_prices` AS
WITH base AS (
  SELECT period, state_id, state_name, sector_id, sector_name,
         price_cents_kwh, sales_mwh, customers
  FROM `{{project}}.eia_staging.retail_prices`
  WHERE sector_id = 'ALL'
    AND price_cents_kwh IS NOT NULL
),
rolling_stats AS (
  SELECT
    *,
    AVG(price_cents_kwh) OVER (
      PARTITION BY state_id ORDER BY period
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_12m,
    STDDEV_SAMP(price_cents_kwh) OVER (
      PARTITION BY state_id ORDER BY period
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_std_12m
  FROM base
),
scored AS (
  SELECT
    *,
    CASE
      WHEN rolling_std_12m = 0 OR rolling_std_12m IS NULL THEN 0
      ELSE (price_cents_kwh - rolling_avg_12m) / rolling_std_12m
    END AS z_score
  FROM rolling_stats
)
SELECT
  period,
  SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(period, '-01')) AS period_date,
  state_id, state_name, sector_id, sector_name,
  price_cents_kwh, sales_mwh, customers,
  rolling_avg_12m, rolling_std_12m,
  z_score,
  ABS(z_score) > 2.5 AS is_anomaly
FROM scored;


-- Monthly generation by fuel type. Aggregate codes are excluded to avoid double counting.
CREATE OR REPLACE TABLE `{{project}}.eia_marts.fact_generation` AS
SELECT
  period,
  SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(period, '-01')) AS period_date,
  state_id,
  fuel_type_code,
  fuel_type_desc,
  CASE
    WHEN fuel_type_code = 'NGO'                                  THEN 'Natural Gas'
    WHEN fuel_type_code IN ('WNT','WNS')                         THEN 'Wind'
    WHEN fuel_type_code IN ('SPV','STH','GEO')                   THEN 'Solar & Geothermal'
    WHEN fuel_type_code = 'NUC'                                  THEN 'Nuclear'
    WHEN fuel_type_code IN ('SUB','LIG','BIT','ANT','RC','WOC')  THEN 'Coal'
    WHEN fuel_type_code IN ('HYC','HPS')                         THEN 'Hydro'
    ELSE 'Other'
  END AS fuel_category,
  generation_mwh,
  SAFE_DIVIDE(
    generation_mwh,
    SUM(generation_mwh) OVER (PARTITION BY period, state_id)
  ) AS generation_share
FROM `{{project}}.eia_staging.generation`
WHERE generation_mwh IS NOT NULL
  AND fuel_type_code NOT IN (
    'ALL','FOS','REN','COW','AOR','BIS','SUN','TSN','TPV','DPV','NG','WND','COL'
  );


CREATE OR REPLACE TABLE `{{project}}.eia_marts.dim_state` AS
SELECT DISTINCT state_id, state_name
FROM `{{project}}.eia_staging.retail_prices`
WHERE LENGTH(state_id) = 2
ORDER BY state_id;


-- Demand heatmap: average demand by hour of day and month
CREATE OR REPLACE TABLE `{{project}}.eia_marts.fact_rto_heatmap` AS
SELECT
  DATE_TRUNC(DATE(period), MONTH) AS month,
  EXTRACT(HOUR FROM period)       AS hour_of_day,
  region_id,
  region_name,
  AVG(demand_mwh)                 AS avg_demand_mwh
FROM `{{project}}.eia_staging.rto_demand`
GROUP BY 1, 2, 3, 4;


-- Regional demand: monthly average and peak per grid operator
CREATE OR REPLACE TABLE `{{project}}.eia_marts.fact_rto_regional` AS
SELECT
  DATE_TRUNC(DATE(period), MONTH) AS month,
  region_id,
  region_name,
  AVG(demand_mwh)                 AS avg_demand_mwh,
  MAX(demand_mwh)                 AS peak_demand_mwh
FROM `{{project}}.eia_staging.rto_demand`
GROUP BY 1, 2, 3;


-- Anomaly leaderboard: anomaly count per state
CREATE OR REPLACE TABLE `{{project}}.eia_marts.fact_anomaly_leaderboard` AS
SELECT state_id, state_name, COUNT(*) AS anomaly_count
FROM `{{project}}.eia_marts.fact_retail_prices`
WHERE is_anomaly
  AND LENGTH(state_id) = 2
GROUP BY 1, 2
ORDER BY anomaly_count DESC;
