-- EIA's region-data endpoint returns several series per region and hour. The 'type' field says which:
-- D = demand, DF = day-ahead forecast, NG = net generation, TI = total interchange.
-- Only D is demand, so only D is kept. (The old Snowflake version averaged all of them together.)
-- Hourly periods look like 2026-03-01T05. The slicing fallback handles other shapes.
MERGE `{{project}}.eia_staging.rto_demand` AS tgt
USING (
  SELECT
    period,
    region_id,
    MAX(region_name)  AS region_name,
    AVG(value)        AS demand_mwh,
    MAX(ingested_at)  AS ingested_at
  FROM (
    SELECT
      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H', JSON_VALUE(payload, '$.period')),
        SAFE_CAST(CONCAT(SUBSTR(JSON_VALUE(payload, '$.period'), 1, 10), ' ',
                         SUBSTR(JSON_VALUE(payload, '$.period'), 12, 2), ':00:00') AS TIMESTAMP),
        SAFE_CAST(JSON_VALUE(payload, '$.period') AS TIMESTAMP)
      )                                                         AS period,
      JSON_VALUE(payload, '$.respondent')                       AS region_id,
      JSON_VALUE(payload, '$."respondent-name"')                AS region_name,
      SAFE_CAST(JSON_VALUE(payload, '$.value') AS FLOAT64)      AS value,
      ingested_at
    FROM `{{project}}.eia_raw.rto_demand`
    WHERE COALESCE(JSON_VALUE(payload, '$.type'), 'D') = 'D'
  )
  WHERE period IS NOT NULL AND region_id IS NOT NULL
  GROUP BY period, region_id
) AS src
ON  tgt.period    = src.period
AND tgt.region_id = src.region_id
WHEN MATCHED THEN UPDATE SET
  region_name = src.region_name,
  demand_mwh  = src.demand_mwh,
  ingested_at = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
  period, region_id, region_name, demand_mwh, ingested_at
) VALUES (
  src.period, src.region_id, src.region_name, src.demand_mwh, src.ingested_at
);
