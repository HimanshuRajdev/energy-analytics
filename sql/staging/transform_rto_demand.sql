MERGE INTO EIA_ENERGY.STAGING.RTO_DEMAND AS tgt
USING (
    SELECT
        PARSE_JSON(r.payload):period::TIMESTAMP_NTZ      AS period,
        PARSE_JSON(r.payload):respondent::VARCHAR(50)    AS region_id,
        MAX(PARSE_JSON(r.payload):"respondent-name"::VARCHAR) AS region_name,
        AVG(PARSE_JSON(r.payload):value::FLOAT)          AS demand_mwh,
        MAX(r.ingested_at)                               AS ingested_at
    FROM EIA_ENERGY.RAW.RTO_DEMAND r
    WHERE PARSE_JSON(r.payload):period IS NOT NULL
    GROUP BY 1, 2
) AS src
ON  tgt.period    = src.period
AND tgt.region_id = src.region_id
WHEN MATCHED THEN UPDATE SET
    tgt.region_name = src.region_name,
    tgt.demand_mwh  = src.demand_mwh,
    tgt.ingested_at = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    period, region_id, region_name, demand_mwh, ingested_at
) VALUES (
    src.period, src.region_id, src.region_name, src.demand_mwh, src.ingested_at
);
