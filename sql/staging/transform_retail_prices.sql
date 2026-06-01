MERGE INTO EIA_ENERGY.STAGING.RETAIL_PRICES AS tgt
USING (
    SELECT DISTINCT
        PARSE_JSON(r.payload):period::VARCHAR(7)        AS period,
        PARSE_JSON(r.payload):stateid::VARCHAR(10)      AS state_id,
        PARSE_JSON(r.payload):stateDescription::VARCHAR AS state_name,
        PARSE_JSON(r.payload):sectorid::VARCHAR(20)     AS sector_id,
        PARSE_JSON(r.payload):sectorName::VARCHAR       AS sector_name,
        PARSE_JSON(r.payload):price::FLOAT              AS price_cents_kwh,
        PARSE_JSON(r.payload):sales::FLOAT              AS sales_mwh,
        PARSE_JSON(r.payload):customers::BIGINT         AS customers,
        MAX(r.ingested_at) AS ingested_at
    FROM EIA_ENERGY.RAW.RETAIL_PRICES r
    WHERE PARSE_JSON(r.payload):period IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8
) AS src
ON  tgt.period    = src.period
AND tgt.state_id  = src.state_id
AND tgt.sector_id = src.sector_id
WHEN MATCHED THEN UPDATE SET
    tgt.state_name      = src.state_name,
    tgt.sector_name     = src.sector_name,
    tgt.price_cents_kwh = src.price_cents_kwh,
    tgt.sales_mwh       = src.sales_mwh,
    tgt.customers       = src.customers,
    tgt.ingested_at     = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    period, state_id, state_name, sector_id, sector_name,
    price_cents_kwh, sales_mwh, customers, ingested_at
) VALUES (
    src.period, src.state_id, src.state_name, src.sector_id, src.sector_name,
    src.price_cents_kwh, src.sales_mwh, src.customers, src.ingested_at
);