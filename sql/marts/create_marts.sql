-- sql/marts/create_marts.sql
-- MARTS = final analytics-ready tables that Tableau reads directly.
-- Includes a 12-month rolling z-score for anomaly detection.

CREATE SCHEMA IF NOT EXISTS EIA_ENERGY.MARTS;

-- Fact table: monthly retail prices with anomaly flag
-- Z-score > 2.5 or < -2.5 = anomaly
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.FACT_RETAIL_PRICES AS
WITH base AS (
    SELECT
        period,
        state_id,
        state_name,
        sector_id,
        sector_name,
        price_cents_kwh,
        sales_mwh,
        customers
    FROM EIA_ENERGY.STAGING.RETAIL_PRICES
    WHERE sector_id = 'ALL'       -- use the all-sectors aggregate for the price trend
      AND price_cents_kwh IS NOT NULL
),
rolling_stats AS (
    SELECT
        *,
        AVG(price_cents_kwh) OVER (
            PARTITION BY state_id
            ORDER BY period
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_12m,
        STDDEV(price_cents_kwh) OVER (
            PARTITION BY state_id
            ORDER BY period
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_std_12m
    FROM base
)
SELECT
    period,
    state_id,
    state_name,
    sector_id,
    sector_name,
    price_cents_kwh,
    sales_mwh,
    customers,
    rolling_avg_12m,
    rolling_std_12m,
    -- z-score: how many standard deviations from the 12-month rolling mean
    CASE
        WHEN rolling_std_12m = 0 OR rolling_std_12m IS NULL THEN 0
        ELSE (price_cents_kwh - rolling_avg_12m) / rolling_std_12m
    END AS z_score,
    -- flag anything beyond 2.5 standard deviations as an anomaly
    CASE
        WHEN ABS(
            CASE
                WHEN rolling_std_12m = 0 OR rolling_std_12m IS NULL THEN 0
                ELSE (price_cents_kwh - rolling_avg_12m) / rolling_std_12m
            END
        ) > 2.5 THEN TRUE ELSE FALSE
    END AS is_anomaly
FROM rolling_stats;


-- Fact table: monthly generation by fuel type
-- Excludes aggregate/duplicate fuel codes. generation_share calculated on filtered rows only
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.FACT_GENERATION AS
SELECT
    period,
    state_id,
    fuel_type_code,
    fuel_type_desc,
    CASE
        WHEN fuel_type_code = 'NGO'                          THEN 'Natural Gas'
        WHEN fuel_type_code IN ('WNT','WNS')                 THEN 'Wind'
        WHEN fuel_type_code IN ('SPV','STH','GEO')           THEN 'Solar & Geothermal'
        WHEN fuel_type_code = 'NUC'                          THEN 'Nuclear'
        WHEN fuel_type_code IN ('SUB','LIG','BIT','ANT','RC','WOC') THEN 'Coal'
        WHEN fuel_type_code IN ('HYC','HPS')                 THEN 'Hydro'
        ELSE 'Other'
    END AS fuel_category,
    generation_mwh,
    generation_mwh / NULLIF(
        SUM(generation_mwh) OVER (PARTITION BY period, state_id), 0
    ) AS generation_share
FROM EIA_ENERGY.STAGING.GENERATION
WHERE generation_mwh IS NOT NULL
  AND fuel_type_code NOT IN (
    'ALL','FOS','REN','COW','AOR','BIS','SUN','TSN','TPV','DPV','NG','WND','COL'
  );


-- Dimension table: states (for Tableau map joins)
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.DIM_STATE AS
SELECT DISTINCT
    state_id,
    state_name
FROM EIA_ENERGY.STAGING.RETAIL_PRICES
WHERE LENGTH(state_id) = 2   -- exclude census region aggregates
ORDER BY state_id;


-- Demand heatmap: avg demand by hour of day and month
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.FACT_RTO_HEATMAP AS
SELECT
    DATE_TRUNC('month', period)  AS month,
    HOUR(period)                 AS hour_of_day,
    region_id,
    region_name,
    AVG(demand_mwh)              AS avg_demand_mwh
FROM EIA_ENERGY.STAGING.RTO_DEMAND
GROUP BY 1, 2, 3, 4;


-- Regional demand comparison: monthly avg per RTO region
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.FACT_RTO_REGIONAL AS
SELECT
    DATE_TRUNC('month', period)  AS month,
    region_id,
    region_name,
    AVG(demand_mwh)              AS avg_demand_mwh,
    MAX(demand_mwh)              AS peak_demand_mwh
FROM EIA_ENERGY.STAGING.RTO_DEMAND
GROUP BY 1, 2, 3;


-- Anomaly leaderboard: count of anomalies per state
CREATE OR REPLACE TABLE EIA_ENERGY.MARTS.FACT_ANOMALY_LEADERBOARD AS
SELECT
    state_id,
    state_name,
    COUNT(*) AS anomaly_count
FROM EIA_ENERGY.MARTS.FACT_RETAIL_PRICES
WHERE is_anomaly = TRUE
AND LENGTH(state_id) = 2
GROUP BY 1, 2
ORDER BY anomaly_count DESC;
