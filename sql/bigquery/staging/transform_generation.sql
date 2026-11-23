MERGE `{{project}}.eia_staging.generation` AS tgt
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      JSON_VALUE(payload, '$.period')                              AS period,
      JSON_VALUE(payload, '$.location')                            AS state_id,
      JSON_VALUE(payload, '$.fueltypeid')                          AS fuel_type_code,
      JSON_VALUE(payload, '$.fuelTypeDescription')                 AS fuel_type_desc,
      SAFE_CAST(JSON_VALUE(payload, '$.generation') AS FLOAT64)    AS generation_mwh,
      ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(payload, '$.period'),
                     JSON_VALUE(payload, '$.location'),
                     JSON_VALUE(payload, '$.fueltypeid')
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `{{project}}.eia_raw.generation`
    WHERE JSON_VALUE(payload, '$.period') IS NOT NULL
  )
  WHERE rn = 1
) AS src
ON  tgt.period         = src.period
AND tgt.state_id       = src.state_id
AND tgt.fuel_type_code = src.fuel_type_code
WHEN MATCHED THEN UPDATE SET
  fuel_type_desc = src.fuel_type_desc,
  generation_mwh = src.generation_mwh,
  ingested_at    = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
  period, state_id, fuel_type_code, fuel_type_desc, generation_mwh, ingested_at
) VALUES (
  src.period, src.state_id, src.fuel_type_code,
  src.fuel_type_desc, src.generation_mwh, src.ingested_at
);
