MERGE INTO EIA_ENERGY.STAGING.GENERATION AS tgt
USING (
    SELECT
        PARSE_JSON(r.payload):period::VARCHAR(7)          AS period,
        PARSE_JSON(r.payload):location::VARCHAR(10)       AS state_id,
        PARSE_JSON(r.payload):fueltypeid::VARCHAR(50)     AS fuel_type_code,
        PARSE_JSON(r.payload):fuelTypeDescription::VARCHAR AS fuel_type_desc,
        PARSE_JSON(r.payload):generation::FLOAT           AS generation_mwh,
        r.ingested_at
    FROM EIA_ENERGY.RAW.GENERATION r
    WHERE PARSE_JSON(r.payload):period IS NOT NULL
) AS src
ON  tgt.period         = src.period
AND tgt.state_id       = src.state_id
AND tgt.fuel_type_code = src.fuel_type_code
WHEN MATCHED THEN UPDATE SET
    tgt.fuel_type_desc = src.fuel_type_desc,
    tgt.generation_mwh = src.generation_mwh,
    tgt.ingested_at    = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    period, state_id, fuel_type_code, fuel_type_desc, generation_mwh, ingested_at
) VALUES (
    src.period, src.state_id, src.fuel_type_code,
    src.fuel_type_desc, src.generation_mwh, src.ingested_at
);
