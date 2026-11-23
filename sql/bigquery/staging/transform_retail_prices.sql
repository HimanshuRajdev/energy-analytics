MERGE `{{project}}.eia_staging.retail_prices` AS tgt
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      JSON_VALUE(payload, '$.period')                          AS period,
      JSON_VALUE(payload, '$.stateid')                         AS state_id,
      JSON_VALUE(payload, '$.stateDescription')                AS state_name,
      JSON_VALUE(payload, '$.sectorid')                        AS sector_id,
      JSON_VALUE(payload, '$.sectorName')                      AS sector_name,
      SAFE_CAST(JSON_VALUE(payload, '$.price')     AS FLOAT64) AS price_cents_kwh,
      SAFE_CAST(JSON_VALUE(payload, '$.sales')     AS FLOAT64) AS sales_mwh,
      SAFE_CAST(JSON_VALUE(payload, '$.customers') AS INT64)   AS customers,
      ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(payload, '$.period'),
                     JSON_VALUE(payload, '$.stateid'),
                     JSON_VALUE(payload, '$.sectorid')
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `{{project}}.eia_raw.retail_prices`
    WHERE JSON_VALUE(payload, '$.period') IS NOT NULL
  )
  WHERE rn = 1
) AS src
ON  tgt.period    = src.period
AND tgt.state_id  = src.state_id
AND tgt.sector_id = src.sector_id
WHEN MATCHED THEN UPDATE SET
  state_name      = src.state_name,
  sector_name     = src.sector_name,
  price_cents_kwh = src.price_cents_kwh,
  sales_mwh       = src.sales_mwh,
  customers       = src.customers,
  ingested_at     = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
  period, state_id, state_name, sector_id, sector_name,
  price_cents_kwh, sales_mwh, customers, ingested_at
) VALUES (
  src.period, src.state_id, src.state_name, src.sector_id, src.sector_name,
  src.price_cents_kwh, src.sales_mwh, src.customers, src.ingested_at
);
